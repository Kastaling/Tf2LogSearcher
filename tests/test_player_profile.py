"""Tests for the player_profile aggregation function."""
import pytest

from app.chat_db import connect_chat_db, init_chat_db, replace_chat_for_log
from app.stats_db import connect_stats_db, init_stats_db, replace_stats_for_log
from app.search.search import player_profile

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
    log_title: str = "Test log",
) -> dict:
    return {
        "info": {
            "map": map_name,
            "date": date_ts,
            "total_length": 300,
            "title": log_title,
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


@pytest.fixture()
def top_logs_combined_excluded_db(stats_db):
    """One combined-map log with inflated stats, one single-map log with lower stats."""
    conn = connect_stats_db(stats_db)
    with conn:
        replace_stats_for_log(
            conn,
            7001,
            _make_logtext(
                PLAYER_A_3,
                PLAYER_B_3,
                map_name="pl_vigil + cp_process",
                a_dmg=999_999,
                a_kills=500,
            ),
        )
        replace_stats_for_log(
            conn,
            7002,
            _make_logtext(
                PLAYER_A_3,
                PLAYER_B_3,
                map_name="cp_process_final",
                date_ts=1_701_000_000,
                a_dmg=4000,
                a_kills=20,
            ),
        )
    conn.close()
    return stats_db


def test_profile_top_logs_excludes_combined_map_uploads(top_logs_combined_excluded_db, monkeypatch):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", top_logs_combined_excluded_db)
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {})

    profile, _ = player_profile(PLAYER_A)
    tl = profile.get("top_logs") or []
    dmg = next((x for x in tl if x.get("metric") == "damage"), None)
    assert dmg is not None
    assert dmg["log_id"] == 7002
    assert dmg["value"] == 4000
    kills = next((x for x in tl if x.get("metric") == "kills"), None)
    assert kills is not None
    assert kills["log_id"] == 7002
    assert kills["value"] == 20


@pytest.fixture()
def top_logs_space_separated_maps_excluded_db(stats_db):
    """Space-separated map tokens (no comma/+) vs a normal single-map log."""
    conn = connect_stats_db(stats_db)
    with conn:
        replace_stats_for_log(
            conn,
            7051,
            _make_logtext(
                PLAYER_A_3,
                PLAYER_B_3,
                map_name="upwd steel cascade",
                a_dmg=999_999,
                a_kills=500,
            ),
        )
        replace_stats_for_log(
            conn,
            7052,
            _make_logtext(
                PLAYER_A_3,
                PLAYER_B_3,
                map_name="koth_bagel_rc2",
                date_ts=1_701_500_000,
                a_dmg=4500,
                a_kills=21,
            ),
        )
    conn.close()
    return stats_db


def test_profile_top_logs_excludes_space_separated_maps(
    top_logs_space_separated_maps_excluded_db,
    monkeypatch,
):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", top_logs_space_separated_maps_excluded_db)
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {})

    profile, _ = player_profile(PLAYER_A)
    dmg = next((x for x in (profile.get("top_logs") or []) if x.get("metric") == "damage"), None)
    assert dmg is not None
    assert dmg["log_id"] == 7052
    assert dmg["value"] == 4500


@pytest.fixture()
def top_logs_placeholder_map_gg_excluded_db(stats_db):
    """Placeholder single-token map (no underscore) vs real ``snake_case`` map."""
    conn = connect_stats_db(stats_db)
    with conn:
        replace_stats_for_log(
            conn,
            7061,
            _make_logtext(
                PLAYER_A_3,
                PLAYER_B_3,
                map_name="GG",
                log_title="PLAT GRAND FINALS (DK VS HOOD) (2-1)",
                a_dmg=999_999,
                a_kills=200,
            ),
        )
        replace_stats_for_log(
            conn,
            7062,
            _make_logtext(
                PLAYER_A_3,
                PLAYER_B_3,
                map_name="koth_bagel",
                date_ts=1_701_600_000,
                a_dmg=8000,
                a_kills=30,
            ),
        )
    conn.close()
    return stats_db


def test_profile_top_logs_excludes_placeholder_map_and_series_title(
    top_logs_placeholder_map_gg_excluded_db,
    monkeypatch,
):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", top_logs_placeholder_map_gg_excluded_db)
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {})

    profile, _ = player_profile(PLAYER_A)
    dmg = next((x for x in (profile.get("top_logs") or []) if x.get("metric") == "damage"), None)
    assert dmg is not None
    assert dmg["log_id"] == 7062
    assert dmg["value"] == 8000


@pytest.fixture()
def top_logs_empty_map_excluded_db(stats_db):
    """Inflated stats on empty map vs lower stats with a real map name."""
    conn = connect_stats_db(stats_db)
    with conn:
        replace_stats_for_log(
            conn,
            7101,
            _make_logtext(
                PLAYER_A_3,
                PLAYER_B_3,
                map_name="",
                a_dmg=888_888,
                a_kills=400,
            ),
        )
        replace_stats_for_log(
            conn,
            7102,
            _make_logtext(
                PLAYER_A_3,
                PLAYER_B_3,
                map_name="cp_process_final",
                date_ts=1_702_000_000,
                a_dmg=5000,
                a_kills=25,
            ),
        )
    conn.close()
    return stats_db


def test_profile_top_logs_excludes_empty_map(top_logs_empty_map_excluded_db, monkeypatch):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", top_logs_empty_map_excluded_db)
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {})

    profile, _ = player_profile(PLAYER_A)
    dmg = next((x for x in (profile.get("top_logs") or []) if x.get("metric") == "damage"), None)
    assert dmg is not None
    assert dmg["log_id"] == 7102
    assert dmg["value"] == 5000


@pytest.fixture()
def top_logs_combined_title_excluded_db(stats_db):
    conn = connect_stats_db(stats_db)
    with conn:
        replace_stats_for_log(
            conn,
            7201,
            _make_logtext(
                PLAYER_A_3,
                PLAYER_B_3,
                map_name="koth_product_rc1",
                log_title="Combined Log",
                a_dmg=777_777,
                a_kills=300,
            ),
        )
        replace_stats_for_log(
            conn,
            7202,
            _make_logtext(
                PLAYER_A_3,
                PLAYER_B_3,
                map_name="cp_snakewater_final1",
                date_ts=1_703_000_000,
                a_dmg=6000,
                a_kills=18,
            ),
        )
    conn.close()
    return stats_db


def test_profile_top_logs_excludes_combined_title(top_logs_combined_title_excluded_db, monkeypatch):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", top_logs_combined_title_excluded_db)
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {})

    profile, _ = player_profile(PLAYER_A)
    k = next((x for x in (profile.get("top_logs") or []) if x.get("metric") == "kills"), None)
    assert k is not None
    assert k["log_id"] == 7202
    assert k["value"] == 18


@pytest.fixture()
def top_logs_chat_combined_excluded_db(tmp_path):
    stats_path = tmp_path / "stats.db"
    chat_path = tmp_path / "chat.db"
    sconn = connect_stats_db(stats_path)
    init_stats_db(sconn)
    cconn = connect_chat_db(chat_path)
    init_chat_db(cconn)
    with sconn:
        replace_stats_for_log(
            sconn,
            7301,
            _make_logtext(
                PLAYER_A_3,
                PLAYER_B_3,
                map_name="cp_process_final",
                a_dmg=600_000,
                a_kills=200,
            ),
        )
        replace_stats_for_log(
            sconn,
            7302,
            _make_logtext(
                PLAYER_A_3,
                PLAYER_B_3,
                map_name="cp_metalworks_f5",
                date_ts=1_704_000_000,
                a_dmg=7000,
                a_kills=22,
            ),
        )
    chat_logtext = {
        "info": {"map": "cp_process_final", "date": 1_700_000_000},
        "chat": [
            {
                "msg": (
                    "The following logs were combined: https://logs.tf/3921375 & "
                    "https://logs.tf/3921388"
                ),
                "name": "Jack's Log Combiner",
                "steamid": "[U:1:500]",
            },
        ],
        "players": {},
    }
    with cconn:
        replace_chat_for_log(cconn, 7301, chat_logtext)
    sconn.close()
    cconn.close()
    return stats_path, chat_path


def test_profile_top_logs_excludes_chat_combined_signature(top_logs_chat_combined_excluded_db, monkeypatch):
    stats_path, chat_path = top_logs_chat_combined_excluded_db
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", stats_path)
    monkeypatch.setattr("app.search.search.CHAT_DB_PATH", chat_path)
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {})

    profile, _ = player_profile(PLAYER_A)
    dmg = next((x for x in (profile.get("top_logs") or []) if x.get("metric") == "damage"), None)
    assert dmg is not None
    assert dmg["log_id"] == 7302
    assert dmg["value"] == 7000


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
