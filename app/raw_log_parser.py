"""
Standalone parser for raw TF2 server log text (Valve `L ...` format).
Extracts position-bearing and round/spawn events for raw_events.db.
"""
from __future__ import annotations

import re
from collections import deque
from datetime import datetime
from typing import Any

from app.steamid_constants import STEAMID64_OFFSET

_STEAMID3_RE = re.compile(r"^\[U:1:(\d+)\]$")

# --- SteamID -----------------------------------------------------------------

_STEAM_LEGACY_RE = re.compile(r"^STEAM_(\d+):(\d+):(\d+)$")


def steamid_to_steamid64(raw: str) -> str | None:
    """
    Convert STEAM_0:X:Y to SteamID64: 76561197960265728 + (Y * 2) + X.
    Also accepts [U:1:N] format.
    Returns 17-digit string or None.
    """
    s = (raw or "").strip()
    if not s:
        return None
    if s.startswith("[U:"):
        m3 = _STEAMID3_RE.match(s)
        if not m3:
            return None
        try:
            account_id = int(m3.group(1))
        except ValueError:
            return None
        return str(STEAMID64_OFFSET + account_id)
    m = _STEAM_LEGACY_RE.match(s)
    if not m:
        return None
    try:
        x = int(m.group(2))
        y = int(m.group(3))
    except ValueError:
        return None
    account_id = y * 2 + x
    return str(STEAMID64_OFFSET + account_id)


# --- Line header & positions -------------------------------------------------

_RE_LINE_TS = re.compile(
    r"^L (\d{2}/\d{2}/\d{4}) - (\d{2}:\d{2}:\d{2}):"
)

# Avoid `.+?:` before the message — timestamps contain colons (21:31:16).
_LOG_PREFIX = r"^L \d{2}/\d{2}/\d{4} - \d{2}:\d{2}:\d{2}:\s*"

_RE_KILL = re.compile(
    _LOG_PREFIX + r'"(.+)" killed "(.+)" with "([^"]*)"'
)

_RE_SUICIDE = re.compile(
    _LOG_PREFIX + r'"(.+)" committed suicide with "([^"]*)"'
)

_RE_KILL_ASSIST = re.compile(
    _LOG_PREFIX + r'"(.+)" triggered "kill assist" against "(.+)"'
)

_RE_UBER = re.compile(
    _LOG_PREFIX + r'"(.+)" triggered "chargedeployed"'
)

_RE_CHARGE_END = re.compile(
    _LOG_PREFIX + r'"(.+)" triggered "chargeended"'
)

_RE_DURATION = re.compile(r'\(duration "([^"]*)"\)')

_RE_CAPTURE = re.compile(
    _LOG_PREFIX + r'"(.+)" triggered "pointcaptured"'
)

_RE_TEAM_CAPTURE = re.compile(
    _LOG_PREFIX + r'Team "[^"]+" triggered "pointcaptured"'
)

_RE_PLAYER_NUM = re.compile(r'\(player(\d+)\s+"([^"]*)"\)')
_RE_POSITION_NUM = re.compile(r'\(position(\d+)\s+"([^"]*)"\)')

_RE_ROUND_WIN = re.compile(
    _LOG_PREFIX + r'(?:World|Team) triggered "Round_Win"'
)

_RE_ROUND_WIN_WINNER = re.compile(r'\(winner "([^"]+)"\)')

_RE_ROUND_START = re.compile(
    _LOG_PREFIX + r'World triggered "Round_Start"'
)

_RE_SPAWN = re.compile(
    _LOG_PREFIX + r'"(.+)" spawned as "([^"]+)"'
)

_RE_POS_GENERIC = re.compile(r'\(position "([^"]*)"\)')
_RE_POS_ATTACKER = re.compile(r'\(attacker_position "([^"]*)"\)')
_RE_POS_VICTIM = re.compile(r'\(victim_position "([^"]*)"\)')
_RE_POS_ASSISTER = re.compile(r'\(assister_position "([^"]*)"\)')
_RE_CP_NUM = re.compile(r'\(cp "([^"]*)"\)')
_RE_CP_NAME = re.compile(r'\(cpname "([^"]*)"\)')

# Third bracket group in "name<uid><steam><team>" — steam id token
_RE_ENTITY_STEAM = re.compile(r"<\d+><([^>]+)><")


def parse_xyz(pos_str: str) -> tuple[int, int, int] | None:
    """Parse '512 -256 192' -> (512, -256, 192). Returns None if unparseable."""
    if not pos_str or not pos_str.strip():
        return None
    parts = pos_str.strip().split()
    if len(parts) != 3:
        return None
    try:
        return int(parts[0]), int(parts[1]), int(parts[2])
    except ValueError:
        return None


def _steam_from_entity(entity: str) -> str | None:
    try:
        m = _RE_ENTITY_STEAM.search(entity)
        if not m:
            return None
        return steamid_to_steamid64(m.group(1).strip())
    except Exception:
        return None


def _xyz_from_match_groups(
    line: str,
    *regexes: re.Pattern[str],
) -> tuple[int | None, int | None, int | None]:
    for rx in regexes:
        m = rx.search(line)
        if m:
            xyz = parse_xyz(m.group(1))
            if xyz:
                return xyz[0], xyz[1], xyz[2]
    return None, None, None


def _parse_ts(line: str) -> datetime | None:
    m = _RE_LINE_TS.match(line)
    if not m:
        return None
    try:
        return datetime.strptime(f"{m.group(1)} {m.group(2)}", "%m/%d/%Y %H:%M:%S")
    except ValueError:
        return None


class _KillRef:
    __slots__ = ("tick", "victim_sid", "row")

    def __init__(self, tick: int, victim_sid: str | None, row: dict[str, Any]) -> None:
        self.tick = tick
        self.victim_sid = victim_sid
        self.row = row


def parse_raw_log(log_id: int, content: str) -> dict[str, list[dict[str, Any]]]:
    """
    Parse raw TF2 server log content.
    Returns dict with keys matching the DB tables:
      kill_events, uber_events, charge_end_events, capture_events, round_events, spawn_events
    """
    del log_id  # reserved for future per-log metadata
    kill_events: list[dict[str, Any]] = []
    uber_events: list[dict[str, Any]] = []
    charge_end_events: list[dict[str, Any]] = []
    capture_events: list[dict[str, Any]] = []
    round_events: list[dict[str, Any]] = []
    spawn_events: list[dict[str, Any]] = []

    first_ts: datetime | None = None
    last_round_start_abs_tick: int | None = None
    recent_kills: deque[_KillRef] = deque(maxlen=20)

    lines = content.splitlines()
    for line in lines:
        try:
            ts = _parse_ts(line)
            if ts is None:
                continue
            if first_ts is None:
                first_ts = ts
            abs_tick = int((ts - first_ts).total_seconds())
            if last_round_start_abs_tick is None:
                round_tick = abs_tick
            else:
                round_tick = abs_tick - last_round_start_abs_tick

            # --- Round start (resets round-relative tick baseline) ---
            if _RE_ROUND_START.search(line):
                last_round_start_abs_tick = abs_tick
                round_events.append(
                    {
                        "tick": abs_tick,
                        "event_type": "round_start",
                        "winner_team": None,
                    }
                )
                round_tick = 0

            # --- Round win ---
            if _RE_ROUND_WIN.search(line):
                wteam: str | None = None
                wm = _RE_ROUND_WIN_WINNER.search(line)
                if wm:
                    wteam = wm.group(1).strip()
                round_events.append(
                    {
                        "tick": abs_tick,
                        "event_type": "round_win",
                        "winner_team": wteam,
                    }
                )

            # --- Point captured (Team …) — one row per capper with positionN ----------------
            if _RE_TEAM_CAPTURE.search(line):
                cpn = _RE_CP_NUM.search(line)
                cname = _RE_CP_NAME.search(line)
                cp_index: int | None = None
                if cpn:
                    try:
                        cp_index = int(cpn.group(1).strip())
                    except ValueError:
                        cp_index = None
                cp_name = cname.group(1) if cname else None
                pos_by_num = {int(n): p for n, p in _RE_POSITION_NUM.findall(line)}
                for num_s, ent in _RE_PLAYER_NUM.findall(line):
                    sid = _steam_from_entity(ent)
                    px, py, pz = None, None, None
                    raw_pos = pos_by_num.get(int(num_s))
                    if raw_pos is not None:
                        xyz = parse_xyz(raw_pos)
                        if xyz:
                            px, py, pz = xyz
                    capture_events.append(
                        {
                            "tick": abs_tick,
                            "round_tick": round_tick,
                            "steamid64": sid,
                            "cp_index": cp_index,
                            "cp_name": cp_name,
                            "pos_x": px,
                            "pos_y": py,
                            "pos_z": pz,
                        }
                    )
                continue

            # --- Suicides (same schema as kills; victim = attacker) ---
            sum_ = _RE_SUICIDE.match(line)
            if sum_:
                ent, weapon = sum_.group(1), sum_.group(2)
                sid = _steam_from_entity(ent)
                ax, ay, az = _xyz_from_match_groups(line, _RE_POS_ATTACKER)
                vx, vy, vz = _xyz_from_match_groups(line, _RE_POS_VICTIM)
                if vx is None and vy is None and vz is None:
                    vx, vy, vz = ax, ay, az
                row = {
                    "tick": abs_tick,
                    "round_tick": round_tick,
                    "attacker_steamid64": sid,
                    "attacker_x": ax,
                    "attacker_y": ay,
                    "attacker_z": az,
                    "victim_steamid64": sid,
                    "victim_x": vx,
                    "victim_y": vy,
                    "victim_z": vz,
                    "assister_steamid64": None,
                    "assister_x": None,
                    "assister_y": None,
                    "assister_z": None,
                    "weapon": weapon or None,
                }
                kill_events.append(row)
                if sid:
                    recent_kills.append(_KillRef(abs_tick, sid, row))
                continue

            # --- Kills ---
            km = _RE_KILL.match(line)
            if km:
                atk_ent, vic_ent, weapon = km.group(1), km.group(2), km.group(3)
                attacker_sid = _steam_from_entity(atk_ent)
                victim_sid = _steam_from_entity(vic_ent)
                ax, ay, az = _xyz_from_match_groups(line, _RE_POS_ATTACKER)
                vx, vy, vz = _xyz_from_match_groups(line, _RE_POS_VICTIM)
                row: dict[str, Any] = {
                    "tick": abs_tick,
                    "round_tick": round_tick,
                    "attacker_steamid64": attacker_sid,
                    "attacker_x": ax,
                    "attacker_y": ay,
                    "attacker_z": az,
                    "victim_steamid64": victim_sid,
                    "victim_x": vx,
                    "victim_y": vy,
                    "victim_z": vz,
                    "assister_steamid64": None,
                    "assister_x": None,
                    "assister_y": None,
                    "assister_z": None,
                    "weapon": weapon or None,
                }
                kill_events.append(row)
                if victim_sid:
                    recent_kills.append(_KillRef(abs_tick, victim_sid, row))
                continue

            # --- Kill assists (correlate to recent kill) ---
            am = _RE_KILL_ASSIST.match(line)
            if am:
                _ass_ent, vic_ent = am.group(1), am.group(2)
                ass_sid = _steam_from_entity(_ass_ent)
                vic_sid = _steam_from_entity(vic_ent)
                sx, sy, sz = _xyz_from_match_groups(line, _RE_POS_ASSISTER)
                if ass_sid and vic_sid:
                    for kr in reversed(recent_kills):
                        if kr.victim_sid != vic_sid:
                            continue
                        if abs(abs_tick - kr.tick) > 2:
                            continue
                        r = kr.row
                        r["assister_steamid64"] = ass_sid
                        r["assister_x"] = sx
                        r["assister_y"] = sy
                        r["assister_z"] = sz
                        break
                continue

            # --- Uber deploy ---
            um = _RE_UBER.match(line)
            if um:
                med_ent = um.group(1)
                med_sid = _steam_from_entity(med_ent)
                px, py, pz = _xyz_from_match_groups(line, _RE_POS_GENERIC)
                uber_events.append(
                    {
                        "tick": abs_tick,
                        "round_tick": round_tick,
                        "medic_steamid64": med_sid,
                        "pos_x": px,
                        "pos_y": py,
                        "pos_z": pz,
                    }
                )
                continue

            # --- Uber / charge finished (natural end of uber) ---
            cem = _RE_CHARGE_END.match(line)
            if cem:
                med_ent = cem.group(1)
                med_sid = _steam_from_entity(med_ent)
                dur: float | None = None
                dm = _RE_DURATION.search(line)
                if dm:
                    try:
                        dur = float(dm.group(1).strip())
                    except ValueError:
                        dur = None
                charge_end_events.append(
                    {
                        "tick": abs_tick,
                        "round_tick": round_tick,
                        "medic_steamid64": med_sid,
                        "duration_sec": dur,
                    }
                )
                continue

            # --- Point captured (legacy: player-triggered) ---
            cm = _RE_CAPTURE.match(line)
            if cm:
                pl_ent = cm.group(1)
                sid = _steam_from_entity(pl_ent)
                cpn = _RE_CP_NUM.search(line)
                cname = _RE_CP_NAME.search(line)
                cp_index: int | None = None
                if cpn:
                    try:
                        cp_index = int(cpn.group(1).strip())
                    except ValueError:
                        cp_index = None
                cp_name = cname.group(1) if cname else None
                px, py, pz = _xyz_from_match_groups(line, _RE_POS_GENERIC)
                capture_events.append(
                    {
                        "tick": abs_tick,
                        "round_tick": round_tick,
                        "steamid64": sid,
                        "cp_index": cp_index,
                        "cp_name": cp_name,
                        "pos_x": px,
                        "pos_y": py,
                        "pos_z": pz,
                    }
                )
                continue

            # --- Spawn ---
            sm = _RE_SPAWN.match(line)
            if sm:
                ent, cls_name = sm.group(1), sm.group(2)
                sid = _steam_from_entity(ent)
                spawn_events.append(
                    {
                        "tick": abs_tick,
                        "round_tick": round_tick,
                        "steamid64": sid,
                        "class_name": (cls_name or "").strip().lower() or None,
                    }
                )
        except Exception:
            continue

    return {
        "kill_events": kill_events,
        "uber_events": uber_events,
        "charge_end_events": charge_end_events,
        "capture_events": capture_events,
        "round_events": round_events,
        "spawn_events": spawn_events,
    }
