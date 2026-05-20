"""Unit tests for search utility helpers (no I/O, no HTTP)."""
from datetime import date, datetime, timezone

import pytest

from app.search.search import (
    _date_range_to_unix_bounds,
    _fts_phrase_query,
    _leaderboard_agg_order_clause,
    _leaderboard_resolve_spec,
    _leaderboard_victim_class_from_lb_key,
    _log_in_date_range,
    _map_matches_query,
    _player_count_filter,
    _winner_team_from_log,
    stats_leaderboard,
)
from app.stats_db import (
    connect_stats_db,
    init_stats_db,
    rebuild_player_stats_agg,
    replace_stats_for_log,
)


PLAYER_A = "76561198000000001"
PLAYER_B = "76561198000000002"
PLAYER_A_3 = "[U:1:39734273]"
PLAYER_B_3 = "[U:1:39734274]"


def _leaderboard_log(
    a_ks: int,
    b_ks: int,
    *,
    log_id: int,
    a_headshots: int = 0,
    a_backstabs: int = 0,
    b_headshots: int = 0,
    b_backstabs: int = 0,
    healspread: dict | None = None,
    classkills: dict | None = None,
) -> dict:
    out = {
        "info": {
            "map": "cp_process_final",
            "date": 1_700_000_000 + log_id,
            "total_length": 300,
            "title": f"Leaderboard test {log_id}",
            "winner": None,
        },
        "teams": {
            "Red": {"score": 3},
            "Blue": {"score": 1},
        },
        "players": {
            PLAYER_A_3: {
                "team": "Red",
                "kills": 10,
                "assists": 2,
                "deaths": 5,
                "dmg": 3000,
                "dapm": 600.0,
                "ubers": 0,
                "drops": 0,
                "headshots": a_headshots,
                "backstabs": a_backstabs,
                "longest_killstreak": a_ks,
                "class_stats": [{"type": "soldier", "total_time": 300, "kills": 10, "assists": 2, "deaths": 5, "dmg": 3000}],
            },
            PLAYER_B_3: {
                "team": "Blue",
                "kills": 8,
                "assists": 1,
                "deaths": 6,
                "dmg": 2400,
                "dapm": 480.0,
                "ubers": 0,
                "drops": 0,
                "headshots": b_headshots,
                "backstabs": b_backstabs,
                "longest_killstreak": b_ks,
                "class_stats": [{"type": "soldier", "total_time": 300, "kills": 8, "assists": 1, "deaths": 6, "dmg": 2400}],
            },
        },
        "names": {
            PLAYER_A_3: "PlayerA",
            PLAYER_B_3: "PlayerB",
        },
    }
    if healspread is not None:
        out["healspread"] = healspread
    if classkills is not None:
        out["classkills"] = classkills
    return out


@pytest.fixture()
def stats_leaderboard_db(tmp_path):
    db_path = tmp_path / "stats.db"
    conn = connect_stats_db(db_path)
    init_stats_db(conn)
    with conn:
        replace_stats_for_log(
            conn, 1001, _leaderboard_log(10, 4, log_id=1001, a_headshots=6, a_backstabs=10, b_headshots=1, b_backstabs=2)
        )
        replace_stats_for_log(
            conn, 1002, _leaderboard_log(2, 4, log_id=1002, a_headshots=4, a_backstabs=0, b_headshots=1, b_backstabs=2)
        )
    rebuild_player_stats_agg(conn)
    conn.close()
    return db_path


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


def test_leaderboard_resolve_spec_dpm_kdr_scopes():
    dpm_t = _leaderboard_resolve_spec("dpm", "total")
    assert "SUM" in dpm_t["order_expr"].upper() and "DAMAGE" in dpm_t["order_expr"].upper()
    dpm_p = _leaderboard_resolve_spec("dpm", "per_log")
    assert "AVG" in dpm_p["select_expr"].upper() and "DAPM" in dpm_p["select_expr"].upper()
    kdr_t = _leaderboard_resolve_spec("kdr", "total")
    assert "SUM(LP.KILLS)" in kdr_t["order_expr"].upper().replace(" ", "")
    kdr_p = _leaderboard_resolve_spec("kdr", "per_log")
    assert kdr_p["value_key"] == "avg_kdr"


def test_leaderboard_agg_order_clause_dpm_kdr_deaths_killstreak():
    assert "total_damage" in (_leaderboard_agg_order_clause("dpm", "total") or "").lower()
    assert "avg_dpm" in (_leaderboard_agg_order_clause("dpm", "per_log") or "").lower()
    assert "total_kills" in (_leaderboard_agg_order_clause("kdr", "total") or "").lower()
    assert "total_deaths" in (_leaderboard_agg_order_clause("avg_deaths", "total") or "").lower()
    assert "total_killstreak" in (_leaderboard_agg_order_clause("avg_killstreak", "total") or "").lower()


def test_leaderboard_resolve_spec_avg_killstreak():
    spec = _leaderboard_resolve_spec("avg_killstreak", "total")
    assert "SUM" in spec["order_expr"].upper()
    assert spec["value_key"] == "total_killstreak"
    spec_p = _leaderboard_resolve_spec("avg_killstreak", "per_log")
    assert spec_p["value_key"] == "avg_killstreak"
    assert spec_p["format"] == "float2"
    assert "DESC" in (_leaderboard_agg_order_clause("avg_killstreak", "per_log") or "").upper()


def test_stats_leaderboard_dpm_per_log_default_scope(stats_leaderboard_db, monkeypatch):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", stats_leaderboard_db)
    rows, _ = stats_leaderboard("dpm", stat_scope="per_log", min_logs=1)
    assert rows[0]["steamid64"] == PLAYER_A
    assert rows[0]["primary_value"] == 600.0


def test_stats_leaderboard_dpm_total_scope(stats_leaderboard_db, monkeypatch):
    """Agg fast path must SELECT ``total_damage`` (not only ``avg_dpm``)."""
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", stats_leaderboard_db)
    rows, _ = stats_leaderboard("dpm", stat_scope="total", min_logs=1)
    assert rows[0]["steamid64"] == PLAYER_A
    assert rows[0]["primary_value"] == 6000


def test_leaderboard_resolve_spec_heals():
    ht = _leaderboard_resolve_spec("heals", "total")
    assert "total_heals" in ht["order_expr"].lower() or "h.total_heals" in ht["order_expr"].lower()
    assert ht["value_key"] == "total_heals"
    hp = _leaderboard_resolve_spec("heals", "per_log")
    assert "avg_heals_per_log" in hp["select_expr"].lower()


@pytest.fixture()
def stats_leaderboard_heals_db(tmp_path):
    db_path = tmp_path / "stats_heals.db"
    conn = connect_stats_db(db_path)
    init_stats_db(conn)
    with conn:
        replace_stats_for_log(
            conn,
            1001,
            _leaderboard_log(
                10,
                4,
                log_id=1001,
                healspread={PLAYER_A_3: {PLAYER_B_3: 1000}, PLAYER_B_3: {PLAYER_A_3: 500}},
            ),
        )
        replace_stats_for_log(
            conn,
            1002,
            _leaderboard_log(
                2,
                4,
                log_id=1002,
                healspread={PLAYER_A_3: {PLAYER_B_3: 3500}, PLAYER_B_3: {PLAYER_A_3: 200}},
            ),
        )
        rebuild_player_stats_agg(conn)
    conn.close()
    return db_path


def test_stats_leaderboard_heals_total_and_per_log(stats_leaderboard_heals_db, monkeypatch):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", stats_leaderboard_heals_db)
    rows_t, _ = stats_leaderboard("heals", min_logs=1)
    assert rows_t[0]["steamid64"] == PLAYER_A
    assert rows_t[0]["primary_value"] == 4500
    assert rows_t[1]["primary_value"] == 700
    rows_p, _ = stats_leaderboard("heals", stat_scope="per_log", min_logs=1)
    assert rows_p[0]["steamid64"] == PLAYER_A
    assert rows_p[0]["primary_value"] == 2250.0
    assert rows_p[1]["primary_value"] == 350.0
    # Global unfiltered + nonempty agg must not take the agg fast path (heals use healspread).
    rows_agg_path, _ = stats_leaderboard("heals", min_logs=1)
    assert rows_agg_path[0]["primary_value"] == 4500


def test_leaderboard_resolve_spec_headshots_backstabs():
    ht = _leaderboard_resolve_spec("headshots", "total")
    assert "HEADSHOTS" in ht["order_expr"].upper()
    assert ht["value_key"] == "total_headshots"
    hp = _leaderboard_resolve_spec("headshots", "per_log")
    assert "AVG" in hp["select_expr"].upper() and "HEADSHOTS" in hp["select_expr"].upper()
    bt = _leaderboard_resolve_spec("backstabs", "total")
    assert "BACKSTABS" in bt["order_expr"].upper()
    assert bt["value_key"] == "total_backstabs"


def test_leaderboard_agg_order_clause_headshots_backstabs():
    assert "total_headshots" in (_leaderboard_agg_order_clause("headshots", "total") or "").lower()
    assert "log_count" in (_leaderboard_agg_order_clause("headshots", "per_log") or "").lower()
    assert "total_backstabs" in (_leaderboard_agg_order_clause("backstabs", "total") or "").lower()
    assert "log_count" in (_leaderboard_agg_order_clause("backstabs", "per_log") or "").lower()


def test_leaderboard_agg_order_clause_heals():
    assert "total_heals" in (_leaderboard_agg_order_clause("heals", "total") or "").lower()
    assert "heal_log_count" in (_leaderboard_agg_order_clause("heals", "per_log") or "").lower()


def test_stats_leaderboard_headshots_and_backstabs(stats_leaderboard_db, monkeypatch):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", stats_leaderboard_db)
    rows_t, _ = stats_leaderboard("headshots", min_logs=1)
    assert rows_t[0]["steamid64"] == PLAYER_A
    assert rows_t[0]["primary_value"] == 10
    assert rows_t[1]["primary_value"] == 2
    rows_p, _ = stats_leaderboard("headshots", stat_scope="per_log", min_logs=1)
    assert rows_p[0]["primary_value"] == 5.0
    assert rows_p[1]["primary_value"] == 1.0
    rows_bs, _ = stats_leaderboard("backstabs", min_logs=1)
    assert rows_bs[0]["steamid64"] == PLAYER_A
    assert rows_bs[0]["primary_value"] == 10
    assert rows_bs[1]["primary_value"] == 4
    rows_bsp, _ = stats_leaderboard("backstabs", stat_scope="per_log", min_logs=1)
    assert rows_bsp[0]["primary_value"] == 5.0
    assert rows_bsp[1]["primary_value"] == 2.0


def test_stats_leaderboard_avg_killstreak(stats_leaderboard_db, monkeypatch):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", stats_leaderboard_db)

    rows, total_logs = stats_leaderboard("avg_killstreak", stat_scope="per_log", min_logs=1)

    assert total_logs == 2
    assert rows[0]["steamid64"] == PLAYER_A
    assert rows[0]["primary_value"] == 6.0
    assert rows[0]["avg_killstreak"] == 6.0
    assert rows[1]["steamid64"] == PLAYER_B
    assert rows[1]["primary_value"] == 4.0


@pytest.fixture()
def stats_leaderboard_classkills_db(tmp_path):
    db_path = tmp_path / "stats.db"
    conn = connect_stats_db(db_path)
    init_stats_db(conn)
    with conn:
        replace_stats_for_log(
            conn,
            1001,
            _leaderboard_log(
                10,
                4,
                log_id=1001,
                classkills={
                    PLAYER_A_3: {"soldier": 12, "scout": 3},
                    PLAYER_B_3: {"soldier": 5, "medic": 2},
                },
            ),
        )
        replace_stats_for_log(
            conn,
            1002,
            _leaderboard_log(
                2,
                4,
                log_id=1002,
                classkills={
                    PLAYER_A_3: {"soldier": 4},
                    PLAYER_B_3: {"soldier": 8, "scout": 1},
                },
            ),
        )
        rebuild_player_stats_agg(conn)
    conn.close()
    return db_path


def test_leaderboard_victim_class_from_lb_key():
    assert _leaderboard_victim_class_from_lb_key("kills_soldier") == "soldier"
    assert _leaderboard_victim_class_from_lb_key("kills_heavyweapons") == "heavyweapons"
    assert _leaderboard_victim_class_from_lb_key("kills_invalid") is None
    assert _leaderboard_victim_class_from_lb_key("dpm") is None


def test_leaderboard_resolve_spec_kills_soldier():
    tot = _leaderboard_resolve_spec("kills_soldier", "total")
    assert tot["value_key"] == "total_class_kills"
    per = _leaderboard_resolve_spec("kills_soldier", "per_log")
    assert per["value_key"] == "avg_class_kills_per_log"


def test_leaderboard_agg_order_clause_kills_soldier():
    assert "total_kills" in (_leaderboard_agg_order_clause("kills_soldier", "total") or "").lower()
    assert "log_count" in (_leaderboard_agg_order_clause("kills_soldier", "per_log") or "").lower()


def test_stats_leaderboard_kills_soldier_total_and_per_log(
    stats_leaderboard_classkills_db, monkeypatch
):
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", stats_leaderboard_classkills_db)
    rows_t, _ = stats_leaderboard("kills_soldier", min_logs=1)
    assert rows_t[0]["steamid64"] == PLAYER_A
    assert rows_t[0]["primary_value"] == 16
    assert rows_t[1]["steamid64"] == PLAYER_B
    assert rows_t[1]["primary_value"] == 13
    rows_p, _ = stats_leaderboard("kills_soldier", stat_scope="per_log", min_logs=1)
    assert rows_p[0]["primary_value"] == 8.0
    assert rows_p[1]["primary_value"] == 6.5


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
        (8, "4s", True),
        (11, "4s", True),
        (12, "4s", False),
        (6, "4s", False),
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
