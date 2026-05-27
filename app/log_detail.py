"""Build and cache structured payloads for the built-in log detail page."""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.chat_db import connect_chat_db
from app.config import (
    CHAT_DB_PATH,
    LOG_DETAIL_CACHE_DB_PATH,
    LOGS_DIR,
    LOGS_TF_API_BASE,
    RAW_EVENTS_DB_PATH,
    RAW_LOGS_DIR,
    STATS_DB_PATH,
)
from app.log_detail_cache_db import (
    connect_log_detail_cache_db,
    get_cached_section,
    init_log_detail_cache_db,
    set_cached_section,
)
from app.log_links import external_log_url, is_internal_log_links, log_url_for_id
from app.log_utils import team_score, winner_team_from_log
from app.logs_tf import steamid3_to_steamid64
from app.search.search import _class_playtime_for_logmatch, _LOGMATCH_CLASS_TYPES
from app.stats_db import connect_stats_db

logger = logging.getLogger(__name__)

SECTION_VERSIONS: dict[str, str] = {
    "summary": "summary:v1",
    "teams": "teams:v1",
    "players": "players:v1",
    "rounds": "rounds:v1",
    "medics": "medics:v1",
    "class_matrix": "class_matrix:v1",
    "chat": "chat:v1",
    "killstreaks": "killstreaks:v1",
    "raw_availability": "raw_availability:v1",
}

_CHAT_MAX_MESSAGES = 800
_KILLSTREAKS_MAX = 200
_ROUND_EVENTS_MAX = 40
_CLASS_MATRIX_KILLERS_MAX = 48

_build_lock = threading.Lock()
_inflight: dict[int, threading.Event] = {}


def _format_log_date(ts: int | None) -> str:
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime(
            "%m/%d/%Y %I:%M:%S %p %Z"
        )
    except (OSError, ValueError, OverflowError):
        return ""


def _json_file_fingerprint(path: Path) -> str:
    if not path.is_file():
        return "missing"
    try:
        st = path.stat()
        return f"{st.st_mtime_ns}:{st.st_size}"
    except OSError:
        return "missing"


def compute_source_fingerprint(log_id: int) -> str | None:
    """Stable fingerprint from local JSON + indexed DB rows for this log."""
    jp = LOGS_DIR / f"{log_id}.json"
    if not jp.is_file():
        return None
    parts: list[str] = [f"json:{_json_file_fingerprint(jp)}"]

    stats_path = Path(STATS_DB_PATH)
    if stats_path.is_file():
        try:
            conn = connect_stats_db(stats_path)
            try:
                row = conn.execute(
                    "SELECT imported_at FROM logs WHERE log_id = ?",
                    (log_id,),
                ).fetchone()
                lp = conn.execute(
                    "SELECT COUNT(*) FROM log_players WHERE log_id = ?",
                    (log_id,),
                ).fetchone()
                lr = conn.execute(
                    "SELECT COUNT(*) FROM log_rounds WHERE log_id = ?",
                    (log_id,),
                ).fetchone()
                lh = conn.execute(
                    "SELECT COUNT(*) FROM log_player_healspread WHERE log_id = ?",
                    (log_id,),
                ).fetchone()
                imp = int(row[0] or 0) if row else 0
                parts.append(
                    f"stats:{imp}:{int(lp[0] or 0) if lp else 0}:"
                    f"{int(lr[0] or 0) if lr else 0}:{int(lh[0] or 0) if lh else 0}"
                )
            finally:
                conn.close()
        except sqlite3.Error as e:
            logger.debug("stats fingerprint for log %s: %s", log_id, e)
            parts.append("stats:err")

    chat_path = Path(CHAT_DB_PATH)
    if chat_path.is_file():
        try:
            conn = connect_chat_db(chat_path)
            try:
                cl = conn.execute(
                    "SELECT chat_count, imported_at_ts FROM chat_logs WHERE log_id = ?",
                    (log_id,),
                ).fetchone()
                cm = conn.execute(
                    "SELECT COUNT(*), COALESCE(MAX(id), 0) FROM chat_messages WHERE log_id = ?",
                    (log_id,),
                ).fetchone()
                if cl:
                    parts.append(
                        f"chat:{int(cl[0] or 0)}:{int(cl[1] or 0)}:"
                        f"{int(cm[0] or 0) if cm else 0}:{int(cm[1] or 0) if cm else 0}"
                    )
                else:
                    parts.append("chat:0:0:0:0")
            finally:
                conn.close()
        except sqlite3.Error as e:
            logger.debug("chat fingerprint for log %s: %s", log_id, e)
            parts.append("chat:err")

    raw_path = Path(RAW_EVENTS_DB_PATH)
    if raw_path.is_file():
        try:
            conn = sqlite3.connect(raw_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10.0)
            try:
                conn.execute("PRAGMA busy_timeout=10000")
                row = conn.execute(
                    """
                    SELECT imported_at, kill_count, uber_count, capture_count, charge_end_count
                    FROM raw_logs WHERE log_id = ?
                    """,
                    (log_id,),
                ).fetchone()
                if row:
                    parts.append(
                        f"raw:{int(row[0] or 0)}:{int(row[1] or 0)}:"
                        f"{int(row[2] or 0)}:{int(row[3] or 0)}:{int(row[4] or 0)}"
                    )
                else:
                    parts.append("raw:missing")
            finally:
                conn.close()
        except Exception as e:
            logger.debug("raw fingerprint for log %s: %s", log_id, e)
            parts.append("raw:err")

    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return digest[:40]


def load_log_json(log_id: int) -> dict[str, Any] | None:
    path = LOGS_DIR / f"{log_id}.json"
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except (OSError, ValueError, json.JSONDecodeError) as e:
        logger.warning("log detail: cannot read %s: %s", path, e)
        return None
    return data if isinstance(data, dict) else None


def _player_row(
    steamid3: str,
    stats: dict[str, Any],
    names: dict[str, Any],
) -> dict[str, Any]:
    alias_raw = names.get(steamid3) if isinstance(names, dict) else ""
    alias = (str(alias_raw).strip() if alias_raw is not None else "") or steamid3
    team_raw = stats.get("team")
    team = "Red" if team_raw == "Red" else ("Blue" if team_raw == "Blue" else None)
    kills = int(stats.get("kills") or 0)
    deaths = int(stats.get("deaths") or 0)
    assists = int(stats.get("assists") or 0)
    dmg = int(stats.get("dmg") or 0)
    dapm_raw = stats.get("dapm")
    if dapm_raw is not None:
        try:
            dpm = round(float(dapm_raw), 2)
        except (TypeError, ValueError):
            dpm = 0.0
    else:
        dpm = 0.0
    primary = None
    cpt = _class_playtime_for_logmatch(stats)
    if cpt:
        primary = cpt[0].get("class")
    sid64 = steamid3_to_steamid64(steamid3) or ""
    return {
        "steamid3": steamid3,
        "steamid64": sid64,
        "alias": alias,
        "team": team,
        "kills": kills,
        "deaths": deaths,
        "assists": assists,
        "dmg": dmg,
        "dpm": dpm,
        "ubers": int(stats.get("ubers") or 0),
        "drops": int(stats.get("drops") or 0),
        "healing_received": int(stats.get("healing_taken") or stats.get("hr") or 0),
        "captures": int(stats.get("captures") or stats.get("cpc") or 0),
        "headshots_hit": int(stats.get("headshots_hit") or stats.get("headshots") or 0),
        "backstabs": int(stats.get("backstabs") or 0),
        "longest_killstreak": int(stats.get("longest_killstreak") or stats.get("lks") or 0),
        "primary_class": primary,
        "class_playtime": cpt,
        "profile_href": f"/?mode=profile&steamid={sid64}" if sid64 else "",
    }


def _build_summary(log_id: int, logtext: dict[str, Any]) -> dict[str, Any]:
    info = logtext.get("info") if isinstance(logtext.get("info"), dict) else {}
    teams = logtext.get("teams")
    date_ts = None
    try:
        if info.get("date") is not None:
            date_ts = int(info.get("date"))
    except (TypeError, ValueError):
        date_ts = None
    duration = int(info.get("total_length") or logtext.get("length") or 0)
    if duration <= 0:
        try:
            duration = int(info.get("length") or 0)
        except (TypeError, ValueError):
            duration = 0
    winner = winner_team_from_log(logtext)
    return {
        "log_id": log_id,
        "title": str(info.get("title") or "").strip(),
        "map": str(info.get("map") or "").strip(),
        "date_ts": date_ts,
        "date_display": _format_log_date(date_ts),
        "duration_secs": duration,
        "winner": winner,
        "red_score": team_score(teams, "Red"),
        "blue_score": team_score(teams, "Blue"),
        "external_log_url": external_log_url(log_id),
        "log_url": log_url_for_id(log_id),
        "link_mode": "internal" if is_internal_log_links() else "external",
        "num_players": len(logtext.get("players") or {}) if isinstance(logtext.get("players"), dict) else 0,
    }


def _build_teams(logtext: dict[str, Any]) -> dict[str, Any]:
    players = logtext.get("players")
    if not isinstance(players, dict):
        players = {}
    totals: dict[str, dict[str, int]] = {
        "Red": {"kills": 0, "deaths": 0, "assists": 0, "dmg": 0, "ubers": 0, "drops": 0, "captures": 0},
        "Blue": {"kills": 0, "deaths": 0, "assists": 0, "dmg": 0, "ubers": 0, "drops": 0, "captures": 0},
    }
    for _sid, p in players.items():
        if not isinstance(p, dict):
            continue
        team = p.get("team")
        if team not in totals:
            continue
        t = totals[team]
        t["kills"] += int(p.get("kills") or 0)
        t["deaths"] += int(p.get("deaths") or 0)
        t["assists"] += int(p.get("assists") or 0)
        t["dmg"] += int(p.get("dmg") or 0)
        t["ubers"] += int(p.get("ubers") or 0)
        t["drops"] += int(p.get("drops") or 0)
        t["captures"] += int(p.get("captures") or p.get("cpc") or 0)
    teams_meta = logtext.get("teams")
    for key in ("Red", "Blue"):
        if isinstance(teams_meta, dict) and isinstance(teams_meta.get(key), dict):
            sc = team_score(teams_meta, key)
            if sc is not None:
                totals[key]["score"] = sc
    return {"teams": totals}


def _build_players(logtext: dict[str, Any]) -> dict[str, Any]:
    players = logtext.get("players")
    names = logtext.get("names")
    if not isinstance(players, dict):
        return {"players": []}
    if not isinstance(names, dict):
        names = {}
    rows = []
    for sid3, stats in players.items():
        if not isinstance(stats, dict):
            continue
        rows.append(_player_row(str(sid3), stats, names))
    rows.sort(key=lambda r: (-(r.get("dmg") or 0), r.get("alias") or ""))
    return {"players": rows}


def _simplify_round_event(ev: Any) -> dict[str, Any] | None:
    if not isinstance(ev, dict):
        return None
    t = str(ev.get("type") or "").strip()
    if not t:
        return None
    row: dict[str, Any] = {"type": t}
    for key in ("killer", "victim", "medigun", "steamid", "team", "point", "assister"):
        if ev.get(key) is not None:
            row[key] = str(ev[key])
    if ev.get("time") is not None:
        try:
            row["time"] = round(float(ev["time"]), 2)
        except (TypeError, ValueError):
            pass
    return row


def _stats_round_rows(log_id: int) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    path = Path(STATS_DB_PATH)
    if not path.is_file():
        return out
    try:
        conn = connect_stats_db(path)
        try:
            rows = conn.execute(
                """
                SELECT round_idx, duration_secs, winner, first_blood_steamid64,
                       red_kills, blue_kills
                FROM log_rounds WHERE log_id = ?
                ORDER BY round_idx ASC
                """,
                (log_id,),
            ).fetchall()
        finally:
            conn.close()
    except sqlite3.Error:
        return out
    for r in rows:
        if not r:
            continue
        idx = int(r[0] or 0)
        fb = str(r[3] or "").strip() or None
        out[idx] = {
            "duration_secs": int(r[1]) if r[1] is not None else None,
            "winner": str(r[2]) if r[2] else None,
            "first_blood_steamid64": fb,
            "red_kills": int(r[4] or 0),
            "blue_kills": int(r[5] or 0),
        }
    return out


def _build_rounds(log_id: int, logtext: dict[str, Any]) -> dict[str, Any]:
    raw_rounds = logtext.get("rounds")
    db_by_idx = _stats_round_rows(log_id)
    rounds_out: list[dict[str, Any]] = []
    if not isinstance(raw_rounds, list):
        raw_rounds = []
    for i, rnd in enumerate(raw_rounds):
        if not isinstance(rnd, dict):
            continue
        db = db_by_idx.get(i, {})
        duration = rnd.get("duration")
        if duration is None:
            duration = rnd.get("length")
        try:
            dur_i = int(duration) if duration is not None else db.get("duration_secs")
        except (TypeError, ValueError):
            dur_i = db.get("duration_secs")
        winner = rnd.get("winner") or db.get("winner")
        kills_block = rnd.get("kills") if isinstance(rnd.get("kills"), dict) else {}
        events_raw = rnd.get("events") if isinstance(rnd.get("events"), list) else []
        events = []
        for ev in events_raw[:_ROUND_EVENTS_MAX]:
            row = _simplify_round_event(ev)
            if row:
                events.append(row)
        rounds_out.append(
            {
                "round_idx": i,
                "duration_secs": dur_i,
                "winner": str(winner) if winner else None,
                "firstcap": rnd.get("firstcap"),
                "kills": {
                    "Red": int(kills_block.get("Red") or db.get("red_kills") or 0),
                    "Blue": int(kills_block.get("Blue") or db.get("blue_kills") or 0),
                },
                "first_blood_steamid64": db.get("first_blood_steamid64"),
                "events": events,
                "events_truncated": len(events_raw) > _ROUND_EVENTS_MAX,
            }
        )
    return {"rounds": rounds_out}


def _build_medics(logtext: dict[str, Any]) -> dict[str, Any]:
    players = logtext.get("players") if isinstance(logtext.get("players"), dict) else {}
    names = logtext.get("names") if isinstance(logtext.get("names"), dict) else {}
    healspread = logtext.get("healspread") if isinstance(logtext.get("healspread"), dict) else {}
    medics: list[dict[str, Any]] = []

    def healing_done_as_medic(sid3: str) -> int:
        block = healspread.get(sid3)
        if not isinstance(block, dict):
            return 0
        total = 0
        for _pat, amt in block.items():
            try:
                total += int(amt or 0)
            except (TypeError, ValueError):
                continue
        return total

    for sid3, stats in players.items():
        if not isinstance(stats, dict):
            continue
        is_medic = False
        for cs in stats.get("class_stats") or []:
            if isinstance(cs, dict) and cs.get("type") == "medic":
                is_medic = True
                break
        heal_done = healing_done_as_medic(sid3)
        if not is_medic and heal_done <= 0:
            continue
        row = _player_row(sid3, stats, names)
        patients: list[dict[str, Any]] = []
        block = healspread.get(sid3)
        if isinstance(block, dict):
            for pat_sid3, amt in block.items():
                try:
                    healing = int(amt or 0)
                except (TypeError, ValueError):
                    continue
                if healing <= 0:
                    continue
                pat_name = names.get(pat_sid3, pat_sid3) if isinstance(names, dict) else pat_sid3
                patients.append(
                    {
                        "steamid3": str(pat_sid3),
                        "steamid64": steamid3_to_steamid64(str(pat_sid3)) or "",
                        "alias": str(pat_name or pat_sid3),
                        "healing": healing,
                    }
                )
            patients.sort(key=lambda x: -x["healing"])
            patients = patients[:12]
        row["healing_done"] = heal_done
        row["top_patients"] = patients
        medics.append(row)
    medics.sort(key=lambda m: -(m.get("healing_done") or 0))
    return {"medics": medics}


def _build_class_matrix(logtext: dict[str, Any]) -> dict[str, Any]:
    classkills = logtext.get("classkills")
    names = logtext.get("names") if isinstance(logtext.get("names"), dict) else {}
    if not isinstance(classkills, dict) or not classkills:
        return {"killers": [], "victim_classes": list(_LOGMATCH_CLASS_TYPES)}
    killers: list[dict[str, Any]] = []
    victim_set: set[str] = set()
    for killer_sid3, victims in classkills.items():
        if not isinstance(victims, dict):
            continue
        alias = names.get(killer_sid3, killer_sid3) if isinstance(names, dict) else killer_sid3
        by_class: dict[str, int] = {}
        for vclass, cnt in victims.items():
            vc = str(vclass or "").strip().lower()
            if not vc:
                continue
            try:
                n = int(cnt or 0)
            except (TypeError, ValueError):
                continue
            if n <= 0:
                continue
            by_class[vc] = by_class.get(vc, 0) + n
            victim_set.add(vc)
        if by_class:
            killers.append(
                {
                    "steamid3": str(killer_sid3),
                    "steamid64": steamid3_to_steamid64(str(killer_sid3)) or "",
                    "alias": str(alias or killer_sid3),
                    "kills_by_victim_class": by_class,
                }
            )
    killers.sort(key=lambda k: -sum((k.get("kills_by_victim_class") or {}).values()))
    killers = killers[:_CLASS_MATRIX_KILLERS_MAX]
    victim_classes = sorted(victim_set) if victim_set else list(_LOGMATCH_CLASS_TYPES)
    return {"killers": killers, "victim_classes": victim_classes}


def _build_chat(log_id: int, logtext: dict[str, Any]) -> dict[str, Any]:
    messages: list[dict[str, Any]] = []
    truncated = False
    chat_path = Path(CHAT_DB_PATH)
    if chat_path.is_file():
        try:
            conn = connect_chat_db(chat_path)
            try:
                rows = conn.execute(
                    """
                    SELECT message_idx, steamid3, steamid64, alias, team, msg
                    FROM chat_messages
                    WHERE log_id = ?
                    ORDER BY message_idx ASC
                    LIMIT ?
                    """,
                    (log_id, _CHAT_MAX_MESSAGES + 1),
                ).fetchall()
            finally:
                conn.close()
            if len(rows) > _CHAT_MAX_MESSAGES:
                truncated = True
                rows = rows[:_CHAT_MAX_MESSAGES]
            for r in rows:
                team = "Red" if r[4] == "Red" else ("Blue" if r[4] == "Blue" else None)
                messages.append(
                    {
                        "idx": int(r[0] or 0),
                        "steamid3": str(r[1] or ""),
                        "steamid64": str(r[2] or "") if r[2] else (steamid3_to_steamid64(str(r[1] or "")) or ""),
                        "alias": str(r[3] or ""),
                        "team": team,
                        "msg": str(r[5] or ""),
                    }
                )
        except sqlite3.Error as e:
            logger.debug("chat db read for log %s: %s", log_id, e)

    if not messages:
        chat = logtext.get("chat")
        players = logtext.get("players") if isinstance(logtext.get("players"), dict) else {}
        names = logtext.get("names") if isinstance(logtext.get("names"), dict) else {}
        if isinstance(chat, list):
            for i, entry in enumerate(chat):
                if len(messages) >= _CHAT_MAX_MESSAGES:
                    truncated = True
                    break
                if not isinstance(entry, dict):
                    continue
                sid3 = str(entry.get("steamid") or "")
                alias = str(entry.get("name") or names.get(sid3) or sid3)
                team = None
                p = players.get(sid3)
                if isinstance(p, dict):
                    tr = p.get("team")
                    team = "Red" if tr == "Red" else ("Blue" if tr == "Blue" else None)
                messages.append(
                    {
                        "idx": i,
                        "steamid3": sid3,
                        "steamid64": steamid3_to_steamid64(sid3) or "",
                        "alias": alias,
                        "team": team,
                        "msg": str(entry.get("msg") or ""),
                    }
                )

    return {"messages": messages, "truncated": truncated, "count": len(messages)}


def _build_killstreaks(logtext: dict[str, Any]) -> dict[str, Any]:
    ks = logtext.get("killstreaks")
    if not isinstance(ks, list):
        return {"killstreaks": []}
    out: list[dict[str, Any]] = []
    names = logtext.get("names") if isinstance(logtext.get("names"), dict) else {}
    for entry in ks[:_KILLSTREAKS_MAX]:
        if not isinstance(entry, dict):
            continue
        sid3 = str(entry.get("steamid") or entry.get("steamid3") or "")
        alias = str(entry.get("name") or names.get(sid3) or sid3)
        try:
            streak = int(entry.get("killstreak") or entry.get("streak") or entry.get("length") or 0)
        except (TypeError, ValueError):
            streak = 0
        out.append(
            {
                "steamid3": sid3,
                "steamid64": steamid3_to_steamid64(sid3) or "",
                "alias": alias,
                "streak": streak,
            }
        )
    return {"killstreaks": out}


def _build_raw_availability(log_id: int) -> dict[str, Any]:
    zip_path = Path(RAW_LOGS_DIR) / f"log_{log_id}.log.zip"
    has_zip = zip_path.is_file()
    indexed = False
    kill_count = uber_count = capture_count = 0
    imported_at: int | None = None
    raw_path = Path(RAW_EVENTS_DB_PATH)
    if raw_path.is_file():
        try:
            conn = sqlite3.connect(raw_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10.0)
            try:
                conn.execute("PRAGMA busy_timeout=10000")
                row = conn.execute(
                    """
                    SELECT imported_at, kill_count, uber_count, capture_count
                    FROM raw_logs WHERE log_id = ?
                    """,
                    (log_id,),
                ).fetchone()
                if row:
                    indexed = True
                    imported_at = int(row[0] or 0)
                    kill_count = int(row[1] or 0)
                    uber_count = int(row[2] or 0)
                    capture_count = int(row[3] or 0)
            finally:
                conn.close()
        except Exception:
            pass
    return {
        "raw_zip_on_disk": has_zip,
        "raw_zip_url": f"{LOGS_TF_API_BASE}/logs/log_{log_id}.log.zip",
        "events_indexed": indexed,
        "imported_at": imported_at,
        "kill_count": kill_count,
        "uber_count": uber_count,
        "capture_count": capture_count,
        "heatmaps_available": False,
    }


_SECTION_BUILDERS: dict[str, Any] = {
    "summary": lambda lid, lt: _build_summary(lid, lt),
    "teams": lambda _lid, lt: _build_teams(lt),
    "players": lambda _lid, lt: _build_players(lt),
    "rounds": lambda lid, lt: _build_rounds(lid, lt),
    "medics": lambda _lid, lt: _build_medics(lt),
    "class_matrix": lambda _lid, lt: _build_class_matrix(lt),
    "chat": lambda lid, lt: _build_chat(lid, lt),
    "killstreaks": lambda _lid, lt: _build_killstreaks(lt),
    "raw_availability": lambda lid, _lt: _build_raw_availability(lid),
}


def _build_section(log_id: int, logtext: dict[str, Any], section_key: str) -> Any:
    fn = _SECTION_BUILDERS.get(section_key)
    if fn is None:
        return {}
    return fn(log_id, logtext)


def _assemble_payload(log_id: int, sections: dict[str, Any], fingerprint: str) -> dict[str, Any]:
    summary = sections.get("summary") if isinstance(sections.get("summary"), dict) else {}
    return {
        "log_id": log_id,
        "source_fingerprint": fingerprint,
        "built_at": int(time.time()),
        "log_url": summary.get("log_url") or log_url_for_id(log_id),
        "external_log_url": summary.get("external_log_url") or external_log_url(log_id),
        "link_mode": summary.get("link_mode") or ("internal" if is_internal_log_links() else "external"),
        "summary": summary,
        "teams": sections.get("teams") or {},
        "players": sections.get("players") or {},
        "rounds": sections.get("rounds") or {},
        "medics": sections.get("medics") or {},
        "class_matrix": sections.get("class_matrix") or {},
        "chat": sections.get("chat") or {},
        "killstreaks": sections.get("killstreaks") or {},
        "raw_availability": sections.get("raw_availability") or {},
    }


def _build_all_sections_locked(
    log_id: int,
    logtext: dict[str, Any],
    fingerprint: str,
    cache_conn: sqlite3.Connection,
) -> dict[str, Any]:
    built: dict[str, Any] = {}
    for section_key, version in SECTION_VERSIONS.items():
        cached = get_cached_section(
            cache_conn, log_id, section_key, version, fingerprint
        )
        if cached is not None:
            built[section_key] = cached
            continue
        payload = _build_section(log_id, logtext, section_key)
        set_cached_section(
            cache_conn,
            log_id,
            section_key,
            version,
            fingerprint,
            payload,
        )
        built[section_key] = payload
    return _assemble_payload(log_id, built, fingerprint)


def _load_log_and_fingerprint(log_id: int) -> tuple[dict[str, Any], str] | None:
    """Load local JSON, then fingerprint all sources (JSON stat + DB rows)."""
    logtext = load_log_json(log_id)
    if logtext is None:
        return None
    fingerprint = compute_source_fingerprint(log_id)
    if fingerprint is None:
        return None
    return logtext, fingerprint


def get_log_detail_payload(log_id: int) -> dict[str, Any] | None:
    """Load or build the full log detail API payload (local library only)."""
    with _build_lock:
        evt = _inflight.get(log_id)
        if evt is not None:
            leader = False
        else:
            evt = threading.Event()
            _inflight[log_id] = evt
            leader = True

    if not leader:
        evt.wait(timeout=600)
        return _build_all_sections_quick(log_id)

    try:
        loaded = _load_log_and_fingerprint(log_id)
        if loaded is None:
            return None
        logtext, fingerprint = loaded
        cache_conn = connect_log_detail_cache_db(LOG_DETAIL_CACHE_DB_PATH)
        try:
            init_log_detail_cache_db(cache_conn)
            return _build_all_sections_locked(log_id, logtext, fingerprint, cache_conn)
        finally:
            cache_conn.close()
    finally:
        evt.set()
        with _build_lock:
            if _inflight.get(log_id) is evt:
                _inflight.pop(log_id, None)


def _build_all_sections_quick(log_id: int) -> dict[str, Any] | None:
    loaded = _load_log_and_fingerprint(log_id)
    if loaded is None:
        return None
    logtext, fingerprint = loaded
    cache_conn = connect_log_detail_cache_db(LOG_DETAIL_CACHE_DB_PATH)
    try:
        init_log_detail_cache_db(cache_conn)
        return _build_all_sections_locked(log_id, logtext, fingerprint, cache_conn)
    finally:
        cache_conn.close()
