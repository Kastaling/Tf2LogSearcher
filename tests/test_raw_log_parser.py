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


# --- parse_raw_log: empty ---


def test_parse_raw_log_empty() -> None:
    out = parse_raw_log(42, "")
    assert set(out.keys()) == {
        "kill_events",
        "uber_events",
        "charge_end_events",
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
