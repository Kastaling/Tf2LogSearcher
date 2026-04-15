"""Tests for the player_profile aggregation function."""
import pytest

from app.stats_db import connect_stats_db, init_stats_db, replace_stats_for_log
from app.search.search import _map_canonical_key, player_profile

PLAYER_A = "76561198000000001"
PLAYER_B = "76561198000000002"
# SteamID3 = [U:1:(steamid64 - 76561197960265728)]
PLAYER_A_3 = "[U:1:39734273]"
PLAYER_B_3 = "[U:1:39734274]"


def _make_logtext(
    player_a_sid3: str,
    player_b_sid3: str,
    a_team: str = "Red",
    b_team: str = "Blue",
    red_score: int = 3,
    blue_score: int = 1,
    map_name: str = "cp_process_final",
    date_ts: int = 1_700_000_000,
    a_kills: int = 12,
    a_deaths: int = 6,
    a_dmg: int = 3600,
    a_ubers: int = 0,
) -> dict:
    return {
        "info": {
            "map": map_name,
            "date": date_ts,
            "total_length": 300,
            "title": "Test log",
            "winner": None,
        },
        "teams": {
            "Red": {"score": red_score},
            "Blue": {"score": blue_score},
        },
        "players": {
            player_a_sid3: {
                "team": a_team,
                "kills": a_kills,
                "assists": 3,
                "deaths": a_deaths,
                "dmg": a_dmg,
                "dapm": round(a_dmg / 300, 2),
                "damage_taken": 2800,
                "healing_taken": 0,
                "ubers": a_ubers,
                "drops": 0,
                "headshots": 0,
                "headshots_hit": 0,
                "backstabs": 0,
                "captures": 1,
                "captures_blocked": 0,
                "dominated": 1,
                "revenges": 0,
                "suicides": 0,
                "longest_killstreak": 4,
                "class_stats": [
                    {"type": "soldier", "total_time": 300, "kills": a_kills,
                     "assists": 3, "deaths": a_deaths, "dmg": a_dmg}
                ],
                "weapon": {
                    "tf_projectile_rocket": {
                        "kills": a_kills, "dmg": a_dmg,
                        "avg_dmg": round(a_dmg / max(a_kills, 1), 1),
                        "shots": 80, "hits": 35
                    }
                },
            },
            player_b_sid3: {
                "team": b_team,
                "kills": 8,
                "assists": 2,
                "deaths": 5,
                "dmg": 2400,
                "dapm": 480.0,
                "damage_taken": 2200,
                "healing_taken": 0,
                "ubers": 0,
                "drops": 0,
                "headshots": 0,
                "headshots_hit": 0,
                "backstabs": 0,
                "captures": 0,
                "captures_blocked": 0,
                "dominated": 0,
                "revenges": 0,
                "suicides": 0,
                "longest_killstreak": 3,
                "class_stats": [
                    {"type": "soldier", "total_time": 300, "kills": 8,
                     "assists": 2, "deaths": 5, "dmg": 2400}
                ],
                "weapon": {},
            },
        },
        "names": {
            player_a_sid3: "PlayerA",
            player_b_sid3: "PlayerB",
        },
        "rounds": [
            {"duration": 90, "winner": "Red", "firstcap": None,
             "kills": {"Red": 8, "Blue": 4}},
            {"duration": 70, "winner": "Blue", "firstcap": None,
             "kills": {"Red": 5, "Blue": 6}},
        ],
    }


@pytest.fixture()
def stats_db(tmp_path):
    db_path = tmp_path / "stats.db"
    conn = connect_stats_db(db_path)
    init_stats_db(conn)
    conn.close()
    return db_path


@pytest.fixture()
def populated_db(stats_db):
    conn = connect_stats_db(stats_db)
    with conn:
        replace_stats_for_log(conn, 1001, _make_logtext(PLAYER_A_3, PLAYER_B_3, red_score=3, blue_score=1))
        replace_stats_for_log(conn, 1002, _make_logtext(PLAYER_A_3, PLAYER_B_3, red_score=1, blue_score=3,
                                                         date_ts=1_700_100_000))
    conn.close()
    return stats_db


def test_profile_overview_counts(populated_db, monkeypatch):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", populated_db)
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {PLAYER_A: "PlayerA"})

    profile, log_ids = player_profile(PLAYER_A)
    assert profile["logs_count"] == 2
    assert profile["steamid64"] == PLAYER_A
    assert len(profile.get("trend_rows") or []) == 2
    ov = profile["overview"]
    assert ov["total_kills"] == 24   # 12 + 12
    assert ov["logs_count"] == 2
    assert ov["best_killstreak"] == 4
    assert ov["first_log_id"] == 1001
    assert ov["last_log_id"] == 1002
    assert 1001 in log_ids and 1002 in log_ids


@pytest.fixture()
def maps_consolidation_db(stats_db):
    conn = connect_stats_db(stats_db)
    with conn:
        replace_stats_for_log(conn, 3001, _make_logtext(PLAYER_A_3, PLAYER_B_3, map_name="pl_vigil_rc9"))
        replace_stats_for_log(
            conn,
            3002,
            _make_logtext(PLAYER_A_3, PLAYER_B_3, map_name="pl_vigil_rc10", date_ts=1_700_200_000),
        )
    conn.close()
    return stats_db


def test_profile_top_maps_consolidation(maps_consolidation_db, monkeypatch):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", maps_consolidation_db)
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {})

    profile, _ = player_profile(PLAYER_A)
    tm = profile.get("top_maps") or []
    vig = next((x for x in tm if x.get("map_key") == "pl_vigil"), None)
    assert vig is not None
    assert vig["logs_count"] == 2
    assert len(vig["versions"]) == 2
    assert sum(v["logs_count"] for v in vig["versions"]) == 2


def test_profile_top_maps_grouped_and_pct(populated_db, monkeypatch):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", populated_db)
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {})

    profile, _ = player_profile(PLAYER_A)
    tm = profile.get("top_maps") or []
    assert len(tm) >= 1
    proc = next((x for x in tm if x["map_key"] == "cp_process"), None)
    assert proc is not None
    assert proc["map_label"] == "cp_process"
    assert proc["logs_count"] == 2
    assert proc["pct_of_total"] == 1.0
    assert len(proc["versions"]) == 1


def test_map_canonical_key_competitive_suffixes():
    assert _map_canonical_key("cp_process_f12") == "cp_process"
    assert _map_canonical_key("cp_process_final") == "cp_process"
    assert _map_canonical_key("cp_process_f9a") == "cp_process"
    assert _map_canonical_key("cp_gullywash_final1") == "cp_gullywash"
    assert _map_canonical_key("cp_gullywash_f9") == "cp_gullywash"
    assert _map_canonical_key("cp_metalworks_f5") == "cp_metalworks"
    assert _map_canonical_key("koth_product_rcx") == "koth_product"
    assert _map_canonical_key("koth_product_final") == "koth_product"
    assert _map_canonical_key("pl_vigil_rc9") == "pl_vigil"
    assert _map_canonical_key("cp_sultry_b8a") == "cp_sultry"
    assert _map_canonical_key("koth_clearcut_b15d") == "koth_clearcut"
    assert _map_canonical_key("koth_clearcut_b15c") == "koth_clearcut"
    assert _map_canonical_key("cp_villa_b17a") == "cp_villa"
    assert _map_canonical_key("koth_cascade_rc1a") == "koth_cascade"
    # Bare ``rc`` / ``r`` are not version tails (avoid ``cp_rc`` → ``cp``).
    assert _map_canonical_key("cp_rc") == "cp_rc"


def test_profile_top_coplayers(populated_db, monkeypatch):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", populated_db)
    monkeypatch.setattr(
        "app.search.search._lookup_aliases_from_chat_db",
        lambda sids: {PLAYER_A: "PlayerA", PLAYER_B: "PlayerB"},
    )

    profile, _ = player_profile(PLAYER_A)
    tcp = profile.get("top_coplayers") or []
    assert len(tcp) >= 1
    b_row = next((x for x in tcp if x["steamid64"] == PLAYER_B), None)
    assert b_row is not None
    assert b_row["total_logs"] == 2
    assert b_row["games_with"] == 0
    assert b_row["games_against"] == 2
    assert b_row["name"] == "PlayerB"

    opp = profile.get("top_coplayers_opposing") or []
    assert len(opp) >= 1
    bo = next((x for x in opp if x["steamid64"] == PLAYER_B), None)
    assert bo is not None
    assert bo["games_against"] == 2
    assert bo["total_logs"] == bo["games_with"] + bo["games_against"]
    assert bo["total_logs"] == 2


def test_profile_classes_section(populated_db, monkeypatch):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", populated_db)
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {})

    profile, _ = player_profile(PLAYER_A)
    classes = profile["classes"]
    assert len(classes) >= 1
    soldier = next((c for c in classes if c["class"] == "soldier"), None)
    assert soldier is not None
    assert soldier["total_kills"] == 24
    assert soldier["total_playtime_secs"] == 600  # 300 * 2 logs


def test_profile_weapons_section(populated_db, monkeypatch):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", populated_db)
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {})

    profile, _ = player_profile(PLAYER_A)
    weapons = profile["weapons"]
    assert len(weapons) >= 1
    rocket = next((w for w in weapons if w["weapon"] == "tf_projectile_rocket"), None)
    assert rocket is not None
    assert rocket["total_kills"] == 24
    assert rocket["logs_count"] == 2


def test_profile_rounds_section(populated_db, monkeypatch):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", populated_db)
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {})

    profile, _ = player_profile(PLAYER_A)
    rounds = profile["rounds"]
    assert rounds["total_rounds"] == 4   # 2 rounds per log × 2 logs
    assert rounds["rounds_with_data"] == 4


def test_profile_not_available_raises(tmp_path, monkeypatch):
    empty_db = tmp_path / "empty.db"
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", empty_db)
    with pytest.raises(RuntimeError, match="Stats DB not available"):
        player_profile(PLAYER_A)


def test_profile_map_filter(populated_db, monkeypatch):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", populated_db)
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {})

    profile, _ = player_profile(PLAYER_A, map_query="granary")
    assert profile["logs_count"] == 0
    assert profile["overview"]["total_kills"] == 0 or profile["overview"]["total_kills"] is None


def test_profile_classes_other_split(populated_db, monkeypatch):
    """Non-standard class labels go to classes_other, not the main nine-class table."""
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", populated_db)
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {})
    conn = connect_stats_db(populated_db)
    with conn:
        conn.execute(
            """
            INSERT INTO log_player_classes (log_id, steamid64, class, playtime, kills, assists, deaths, damage)
            VALUES (1001, ?, 'spectator', 60, 0, 0, 0, 0)
            """,
            (PLAYER_A,),
        )
    conn.close()

    profile, _ = player_profile(PLAYER_A)
    assert any(c.get("class") == "soldier" for c in profile["classes"])
    assert any(c.get("class") == "spectator" for c in profile["classes_other"])


def test_round_duration_reads_length_field(stats_db, monkeypatch):
    """logs.tf API rounds often use ``length`` instead of ``duration`` for seconds."""
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", stats_db)
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {})
    lt = _make_logtext(PLAYER_A_3, PLAYER_B_3)
    lt["rounds"] = [
        {"length": 120, "winner": "Red", "kills": {"Red": 3, "Blue": 2}},
    ]
    conn = connect_stats_db(stats_db)
    init_stats_db(conn)
    with conn:
        replace_stats_for_log(conn, 5001, lt)
    conn.close()

    profile, _ = player_profile(PLAYER_A)
    r = profile["rounds"]
    assert r["total_rounds"] >= 1
    assert r["rounds_with_data"] >= 1
    assert r["avg_round_duration_secs"] == 120.0


def test_first_blood_from_round_events(stats_db, monkeypatch):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", stats_db)
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {})
    lt = _make_logtext(PLAYER_A_3, PLAYER_B_3)
    lt["rounds"] = [
        {
            "duration": 90,
            "winner": "Red",
            "kills": {"Red": 1, "Blue": 0},
            "events": [
                {"type": "kill", "time": 10.0, "killer": PLAYER_A_3},
            ],
        },
    ]
    conn = connect_stats_db(stats_db)
    init_stats_db(conn)
    with conn:
        replace_stats_for_log(conn, 5002, lt)
    conn.close()

    profile, _ = player_profile(PLAYER_A)
    assert profile["rounds"]["first_bloods"] >= 1


def test_profile_win_rate(populated_db, monkeypatch):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", populated_db)
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {})
    # Patch winner into one log to get a deterministic win/loss
    conn = connect_stats_db(populated_db)
    with conn:
        conn.execute("UPDATE logs SET winner = 'Red' WHERE log_id = 1001")
        conn.execute("UPDATE logs SET winner = 'Blue' WHERE log_id = 1002")
    conn.close()

    profile, _ = player_profile(PLAYER_A)  # PlayerA is Red in both logs
    ov = profile["overview"]
    assert ov["wins"] == 1
    assert ov["losses"] == 1
    assert ov["win_rate"] == 0.5
