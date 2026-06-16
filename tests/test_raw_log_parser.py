"""Tests for app.raw_log_parser (TF2 server log parsing; no I/O, no network)."""
from __future__ import annotations

import pytest

from app.raw_log_parser import parse_raw_log, parse_xyz, steamid_to_steamid64
from app.steamid_constants import STEAMID64_OFFSET


def _sid(account: int) -> str:
    return str(STEAMID64_OFFSET + account)


def _ent(name: str, account: int, team: str = "Red") -> str:
    return f'{name}<1><[U:1:{account}]><{team}>'


def _line(mm: int, ss: int, body: str) -> str:
    return f"L 01/02/2024 - 00:{mm:02d}:{ss:02d}: {body}"


# --- steamid_to_steamid64 ---


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("[U:1:0]", _sid(0)),
        ("[U:1:12345]", _sid(12345)),
        ("  [U:1:99]  ", _sid(99)),
        ("STEAM_0:0:1", _sid(2)),  # y*2+x = 1*2+0
        ("STEAM_0:1:0", _sid(1)),  # 0*2+1
    ],
)
def test_steamid_to_steamid64_valid(raw: str, expected: str) -> None:
    assert steamid_to_steamid64(raw) == expected


@pytest.mark.parametrize(
    "raw",
    ["", "   ", "[U:2:1]", "[U:1:abc]", "not_a_steamid", "STEAM_0:0", "STEAM_0:x:y"],
)
def test_steamid_to_steamid64_invalid(raw: str) -> None:
    assert steamid_to_steamid64(raw) is None


# --- parse_xyz ---


@pytest.mark.parametrize(
    "s,expected",
    [
        ("512 -256 192", (512, -256, 192)),
        ("0 0 0", (0, 0, 0)),
        ("  10  20  30  ", (10, 20, 30)),
    ],
)
def test_parse_xyz_ok(s: str, expected: tuple[int, int, int]) -> None:
    assert parse_xyz(s) == expected


@pytest.mark.parametrize(
    "s",
    ["", "  ", "1 2", "1 2 3 4", "a b c", "1.5 0 0"],
)
def test_parse_xyz_bad(s: str) -> None:
    assert parse_xyz(s) is None


def test_parse_xyz_rejects_sqlite_overflow() -> None:
    assert parse_xyz("9223372036854775808 0 0") is None


# --- parse_raw_log: empty ---


def test_parse_raw_log_empty() -> None:
    out = parse_raw_log(42, "")
    assert set(out.keys()) == {
        "kill_events",
        "uber_events",
        "charge_end_events",
        "charge_ready_events",
        "lost_advantage_events",
        "medic_death_events",
        "empty_uber_events",
        "capture_blocked_events",
        "passtime_events",
        "capture_events",
        "round_events",
        "spawn_events",
    }
    for v in out.values():
        assert v == []


def test_parse_raw_log_ignores_non_log_lines() -> None:
    out = parse_raw_log(1, "garbage\nno timestamp here\n")
    assert out["kill_events"] == []


# --- kills & positions ---


def test_parse_raw_log_kill_with_positions() -> None:
    a = _ent("A", 10, "Red")
    b = _ent("B", 20, "Blue")
    log = "\n".join(
        [
            _line(0, 0, f'"{a}" killed "{b}" with "scattergun" (attacker_position "100 200 300") (victim_position "400 500 600")'),
        ]
    )
    out = parse_raw_log(1, log)
    kills = out["kill_events"]
    assert len(kills) == 1
    k = kills[0]
    assert k["attacker_steamid64"] == _sid(10)
    assert k["victim_steamid64"] == _sid(20)
    assert k["weapon"] == "scattergun"
    assert k["tick"] == 0
    assert k["attacker_x"] == 100 and k["attacker_y"] == 200 and k["attacker_z"] == 300
    assert k["victim_x"] == 400 and k["victim_y"] == 500 and k["victim_z"] == 600
    assert k["assister_steamid64"] is None


def test_parse_raw_log_suicide() -> None:
    a = _ent("S", 5, "Red")
    log = _line(0, 5, f'"{a}" committed suicide with "world" (attacker_position "1 2 3")')
    out = parse_raw_log(1, log)
    kills = out["kill_events"]
    assert len(kills) == 1
    k = kills[0]
    assert k["attacker_steamid64"] == _sid(5)
    assert k["victim_steamid64"] == _sid(5)
    assert k["weapon"] == "world"


# --- kill assist correlation ---


def test_parse_raw_log_kill_assist_correlates_within_window() -> None:
    atk = _ent("Atk", 1, "Red")
    vic = _ent("Vic", 2, "Blue")
    ass = _ent("Asst", 3, "Red")
    log = "\n".join(
        [
            _line(0, 0, f'"{atk}" killed "{vic}" with "rocket"'),
            _line(0, 1, f'"{ass}" triggered "kill assist" against "{vic}" (assister_position "9 8 7")'),
        ]
    )
    out = parse_raw_log(1, log)
    assert len(out["kill_events"]) == 1
    k = out["kill_events"][0]
    assert k["assister_steamid64"] == _sid(3)
    assert k["assister_x"] == 9 and k["assister_y"] == 8 and k["assister_z"] == 7


def test_parse_raw_log_kill_assist_not_correlated_if_too_late() -> None:
    atk = _ent("Atk", 1, "Red")
    vic = _ent("Vic", 2, "Blue")
    ass = _ent("Asst", 3, "Red")
    log = "\n".join(
        [
            _line(0, 0, f'"{atk}" killed "{vic}" with "rocket"'),
            # abs_tick diff 3 > 2
            _line(0, 3, f'"{ass}" triggered "kill assist" against "{vic}" (assister_position "1 1 1")'),
        ]
    )
    out = parse_raw_log(1, log)
    k = out["kill_events"][0]
    assert k["assister_steamid64"] is None


# --- rounds ---


def test_parse_raw_log_round_start_and_win() -> None:
    log = "\n".join(
        [
            _line(0, 0, 'World triggered "Round_Start"'),
            _line(0, 1, 'World triggered "Round_Win" (winner "Red")'),
        ]
    )
    out = parse_raw_log(1, log)
    ev = out["round_events"]
    assert len(ev) == 2
    assert ev[0]["event_type"] == "round_start"
    assert ev[0]["winner_team"] is None
    assert ev[1]["event_type"] == "round_win"
    assert ev[1]["winner_team"] == "Red"


# --- uber & charge end ---


def test_parse_raw_log_uber_deploy_position() -> None:
    med = _ent("Medic", 77, "Blue")
    log = _line(0, 0, f'"{med}" triggered "chargedeployed" (position "1 2 3")')
    out = parse_raw_log(1, log)
    u = out["uber_events"]
    assert len(u) == 1
    assert u[0]["medic_steamid64"] == _sid(77)
    assert u[0]["pos_x"] == 1 and u[0]["pos_y"] == 2 and u[0]["pos_z"] == 3


def test_parse_raw_log_charge_end_duration() -> None:
    med = _ent("Medic", 88, "Blue")
    log = _line(0, 0, f'"{med}" triggered "chargeended" (duration "8.5")')
    out = parse_raw_log(1, log)
    ce = out["charge_end_events"]
    assert len(ce) == 1
    assert ce[0]["medic_steamid64"] == _sid(88)
    assert ce[0]["duration_sec"] == 8.5


def test_parse_raw_log_charge_ready() -> None:
    med = _ent("Medic", 88, "Blue")
    log = _line(0, 0, f'"{med}" triggered "chargeready"')
    out = parse_raw_log(1, log)
    cr = out["charge_ready_events"]
    assert len(cr) == 1
    assert cr[0]["medic_steamid64"] == _sid(88)
    assert cr[0]["tick"] == 0


def test_parse_raw_log_lost_advantage() -> None:
    med = _ent("Medic", 88, "Blue")
    log = _line(10, 5, f'"{med}" triggered "lost_uber_advantage" (time "21")')
    out = parse_raw_log(1, log)
    la = out["lost_advantage_events"]
    assert len(la) == 1
    assert la[0]["medic_steamid64"] == _sid(88)
    assert la[0]["advantage_sec"] == 21.0


def test_parse_raw_log_medic_death_with_ex_and_positions() -> None:
    killer = _ent("Killer", 10, "Blue")
    medic = _ent("Medic", 20, "Red")
    log = "\n".join(
        [
            _line(
                0,
                0,
                f'"{killer}" triggered "medic_death" against "{medic}" (healing "1522") (ubercharge "1")',
            ),
            _line(0, 0, f'"{medic}" triggered "medic_death_ex" (uberpct "100")'),
            _line(
                0,
                0,
                f'"{killer}" killed "{medic}" with "iron_bomber" (victim_position "-695 -99 191")',
            ),
        ]
    )
    out = parse_raw_log(1, log)
    md = out["medic_death_events"]
    assert len(md) == 1
    assert md[0]["killer_steamid64"] == _sid(10)
    assert md[0]["medic_steamid64"] == _sid(20)
    assert md[0]["healing"] == 1522
    assert md[0]["had_uber"] == 1
    assert md[0]["uber_pct"] == 100
    assert md[0]["pos_x"] == -695
    assert md[0]["pos_y"] == -99
    assert md[0]["pos_z"] == 191


def test_parse_raw_log_empty_uber() -> None:
    med = _ent("Medic", 88, "Blue")
    log = _line(0, 0, f'"{med}" triggered "empty_uber"')
    out = parse_raw_log(1, log)
    eu = out["empty_uber_events"]
    assert len(eu) == 1
    assert eu[0]["medic_steamid64"] == _sid(88)


def test_parse_raw_log_capture_blocked() -> None:
    pl = _ent("Def", 50, "Red")
    log = _line(
        0,
        0,
        f'"{pl}" triggered "captureblocked" (cp "0") (cpname "the Tower") (position "-168 147 272")',
    )
    out = parse_raw_log(1, log)
    cb = out["capture_blocked_events"]
    assert len(cb) == 1
    assert cb[0]["steamid64"] == _sid(50)
    assert cb[0]["cp_index"] == 0
    assert cb[0]["cp_name"] == "the Tower"
    assert cb[0]["pos_x"] == -168


def test_parse_raw_log_passtime_score_and_caught() -> None:
    scorer = _ent("Scorer", 1, "Red")
    passer = _ent("Passer", 2, "Blue")
    log = "\n".join(
        [
            _line(
                0,
                0,
                f'"{scorer}" triggered "pass_score" (points "1") (dist "285") (speed "933") '
                '(position "-14 -1378 -1775")',
            ),
            _line(
                0,
                5,
                f'"{scorer}" triggered "pass_pass_caught" against "{passer}" (interception "1") '
                '(save "0") (handoff "0") (dist "27.247") (duration "0.630") '
                '(thrower_position "533 789 -1538") (catcher_position "483 787 -1757")',
            ),
        ]
    )
    out = parse_raw_log(1, log)
    pt = out["passtime_events"]
    assert len(pt) == 2
    score = next(e for e in pt if e["event_type"] == "pass_score")
    assert score["steamid64"] == _sid(1)
    assert score["points"] == 1
    assert score["speed"] == 933
    caught = next(e for e in pt if e["event_type"] == "pass_pass_caught")
    assert caught["other_steamid64"] == _sid(2)
    assert caught["interception"] == 1
    assert caught["thrower_pos_x"] == 533
    assert caught["catcher_pos_z"] == -1757


# --- captures ---


def test_parse_raw_log_legacy_player_capture() -> None:
    pl = _ent("Cap", 50, "Red")
    log = _line(
        0,
        0,
        f'"{pl}" triggered "pointcaptured" (cp "2") (cpname "second") (position "100 200 300")',
    )
    out = parse_raw_log(1, log)
    c = out["capture_events"]
    assert len(c) == 1
    assert c[0]["steamid64"] == _sid(50)
    assert c[0]["cp_index"] == 2
    assert c[0]["cp_name"] == "second"
    assert c[0]["pos_x"] == 100


def test_parse_raw_log_team_capture_multiple_players() -> None:
    p1 = _ent("P1", 101, "Red")
    p2 = _ent("P2", 102, "Red")
    log = _line(
        0,
        0,
        'Team "RED" triggered "pointcaptured" (cp "0") (cpname "A") '
        f'(player1 "{p1}") (position1 "10 20 30") (player2 "{p2}") (position2 "40 50 60")',
    )
    out = parse_raw_log(1, log)
    c = out["capture_events"]
    assert len(c) == 2
    sids = sorted([x["steamid64"] for x in c])
    assert sids == sorted([_sid(101), _sid(102)])


# --- spawn ---


def test_parse_raw_log_spawn_class_lower() -> None:
    pl = _ent("Soldier", 33, "Red")
    log = _line(0, 0, f'"{pl}" spawned as "soldier"')
    out = parse_raw_log(1, log)
    s = out["spawn_events"]
    assert len(s) == 1
    assert s[0]["steamid64"] == _sid(33)
    assert s[0]["class_name"] == "soldier"


# --- round_tick after Round_Start ---


def test_parse_raw_log_round_tick_resets() -> None:
    # Same wall time as Round_Start so abs_tick matches; round_tick stays 0 right at round start.
    log = "\n".join(
        [
            _line(0, 0, 'World triggered "Round_Start"'),
            _line(0, 0, f'"{_ent("X", 1)}" spawned as "scout"'),
        ]
    )
    out = parse_raw_log(1, log)
    sp = out["spawn_events"][0]
    assert sp["tick"] == 0
    assert sp["round_tick"] == 0
