"""SQLite storage for per-log player stats from logs.tf JSON (downloader + backfill)."""
from __future__ import annotations

import logging
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Sequence

from app.log_utils import team_score, winner_team_from_log as _winner_team_from_logtext
from app.logs_tf import steamid3_to_steamid64

# Garbage / non-class strings sometimes seen in logs.tf class_stats (skip inserts).
_BAD_CLASS_NAMES: frozenset[str] = frozenset({"", "undefined", "none"})

# SQLite bind parameter limit (stay under 999).
_PSA_CHUNK = 900
# Nested transaction for atomic batch refresh (safe with or without an outer transaction).
_PSA_SAVEPOINT = "psa_refresh"

logger = logging.getLogger(__name__)


def connect_stats_db(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # timeout (seconds) + busy_timeout (ms) must apply before PRAGMA journal_mode, which can contend
    # with the downloader for the DB file.
    conn = sqlite3.connect(str(path), timeout=120.0)
    conn.execute("PRAGMA busy_timeout=120000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _normalize_class_name(raw: Any) -> str:
    return str(raw or "").strip().lower()


def _round_duration_secs_from_log(rnd: dict) -> int | None:
    """logs.tf uses ``duration`` in some payloads and ``length`` in API-shaped JSON."""
    raw = rnd.get("duration")
    if raw is None:
        raw = rnd.get("length")
    if raw is None:
        return None
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return None


def _steamid64_from_logs_tf_player_field(raw: Any) -> str | None:
    """Accept SteamID3 ``[U:1:n]`` or 17-digit SteamID64 string."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if s.startswith("[U:"):
        return steamid3_to_steamid64(s)
    if re.fullmatch(r"\d{17}", s):
        return s
    return None


def _first_blood_steamid64_from_round(rnd: dict) -> str | None:
    """
    First kill of the round from ``events`` (logs.tf). ``firstcap`` is a team, not a player — do not use it.
    """
    for key in ("first_blood", "firstblood", "firstBlood"):
        sid64 = _steamid64_from_logs_tf_player_field(rnd.get(key))
        if sid64:
            return sid64
    evs = rnd.get("events")
    if not isinstance(evs, list):
        return None
    scored: list[tuple[float, dict[str, Any]]] = []
    for ev in evs:
        if not isinstance(ev, dict):
            continue
        try:
            t = float(ev.get("time") or 0)
        except (TypeError, ValueError):
            t = 0.0
        scored.append((t, ev))
    scored.sort(key=lambda x: x[0])
    for _, ev in scored:
        et = str(ev.get("type") or "").lower()
        if "kill" not in et:
            continue
        for kk in ("killer", "steamid", "attacker"):
            sid64 = _steamid64_from_logs_tf_player_field(ev.get(kk))
            if sid64:
                return sid64
    return None


def _int_safe(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except (TypeError, ValueError):
        return default


def _float_safe(x: Any) -> float | None:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def init_stats_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS logs (
          log_id        INTEGER PRIMARY KEY,
          title         TEXT NOT NULL DEFAULT '',
          map           TEXT NOT NULL DEFAULT '',
          date_ts       INTEGER,
          duration_secs INTEGER,
          num_players   INTEGER,
          red_score     INTEGER,
          blue_score    INTEGER,
          winner        TEXT,
          imported_at   INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_logs_date_ts ON logs(date_ts);
        CREATE INDEX IF NOT EXISTS idx_logs_map ON logs(map);
        CREATE INDEX IF NOT EXISTS idx_logs_num_players_date_ts ON logs(num_players, date_ts);

        CREATE TABLE IF NOT EXISTS log_players (
          id             INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id         INTEGER NOT NULL REFERENCES logs(log_id) ON DELETE CASCADE,
          steamid64      TEXT NOT NULL,
          steamid3       TEXT NOT NULL,
          team           TEXT,
          kills          INTEGER NOT NULL DEFAULT 0,
          assists        INTEGER NOT NULL DEFAULT 0,
          deaths         INTEGER NOT NULL DEFAULT 0,
          damage         INTEGER NOT NULL DEFAULT 0,
          damage_taken   INTEGER NOT NULL DEFAULT 0,
          healing_taken  INTEGER NOT NULL DEFAULT 0,
          ubers          INTEGER NOT NULL DEFAULT 0,
          drops          INTEGER NOT NULL DEFAULT 0,
          medigun_ubers  INTEGER NOT NULL DEFAULT 0,
          kritz_ubers    INTEGER NOT NULL DEFAULT 0,
          other_ubers    INTEGER NOT NULL DEFAULT 0,
          headshots      INTEGER NOT NULL DEFAULT 0,
          headshots_hit  INTEGER NOT NULL DEFAULT 0,
          backstabs      INTEGER NOT NULL DEFAULT 0,
          captures       INTEGER NOT NULL DEFAULT 0,
          captures_blocked INTEGER NOT NULL DEFAULT 0,
          dominated      INTEGER NOT NULL DEFAULT 0,
          revenges       INTEGER NOT NULL DEFAULT 0,
          suicides       INTEGER NOT NULL DEFAULT 0,
          longest_killstreak INTEGER NOT NULL DEFAULT 0,
          dapm           REAL,
          kdr            REAL,
          kadr           REAL,
          primary_class  TEXT,
          imported_at    INTEGER NOT NULL,
          UNIQUE(log_id, steamid64)
        );
        CREATE INDEX IF NOT EXISTS idx_log_players_steamid64 ON log_players(steamid64);
        CREATE INDEX IF NOT EXISTS idx_log_players_log_id ON log_players(log_id);
        CREATE INDEX IF NOT EXISTS idx_log_players_steamid64_log_id ON log_players(steamid64, log_id);
        CREATE INDEX IF NOT EXISTS idx_log_players_log_id_team ON log_players(log_id, team);

        CREATE TABLE IF NOT EXISTS log_player_classes (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id      INTEGER NOT NULL REFERENCES logs(log_id) ON DELETE CASCADE,
          steamid64   TEXT NOT NULL,
          class       TEXT NOT NULL,
          playtime    INTEGER NOT NULL DEFAULT 0,
          kills       INTEGER NOT NULL DEFAULT 0,
          assists     INTEGER NOT NULL DEFAULT 0,
          deaths      INTEGER NOT NULL DEFAULT 0,
          damage      INTEGER NOT NULL DEFAULT 0,
          UNIQUE(log_id, steamid64, class)
        );
        CREATE INDEX IF NOT EXISTS idx_lpc_steamid64 ON log_player_classes(steamid64);
        CREATE INDEX IF NOT EXISTS idx_lpc_log_id ON log_player_classes(log_id);
        CREATE INDEX IF NOT EXISTS idx_lpc_steamid64_class ON log_player_classes(steamid64, class);
        CREATE INDEX IF NOT EXISTS idx_lpc_log_steam_class ON log_player_classes(log_id, steamid64, class);

        CREATE TABLE IF NOT EXISTS log_player_weapons (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id      INTEGER NOT NULL REFERENCES logs(log_id) ON DELETE CASCADE,
          steamid64   TEXT NOT NULL,
          weapon      TEXT NOT NULL,
          kills       INTEGER NOT NULL DEFAULT 0,
          damage      INTEGER NOT NULL DEFAULT 0,
          avg_damage  REAL,
          shots       INTEGER NOT NULL DEFAULT 0,
          hits        INTEGER NOT NULL DEFAULT 0,
          UNIQUE(log_id, steamid64, weapon)
        );
        CREATE INDEX IF NOT EXISTS idx_lpw_steamid64 ON log_player_weapons(steamid64);
        CREATE INDEX IF NOT EXISTS idx_lpw_log_id ON log_player_weapons(log_id);
        CREATE INDEX IF NOT EXISTS idx_lpw_weapon ON log_player_weapons(weapon);

        CREATE TABLE IF NOT EXISTS log_player_classkills (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id      INTEGER NOT NULL REFERENCES logs(log_id) ON DELETE CASCADE,
          steamid64   TEXT NOT NULL,
          victim_class TEXT NOT NULL,
          kills       INTEGER NOT NULL DEFAULT 0,
          UNIQUE(log_id, steamid64, victim_class)
        );
        CREATE INDEX IF NOT EXISTS idx_lpck_steamid64 ON log_player_classkills(steamid64);
        CREATE INDEX IF NOT EXISTS idx_lpck_log_id ON log_player_classkills(log_id);
        CREATE INDEX IF NOT EXISTS idx_lpck_victim_class ON log_player_classkills(victim_class);

        CREATE TABLE IF NOT EXISTS log_player_healspread (
          id           INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id       INTEGER NOT NULL REFERENCES logs(log_id) ON DELETE CASCADE,
          healer_steamid64  TEXT NOT NULL,
          patient_steamid64 TEXT NOT NULL,
          healing      INTEGER NOT NULL DEFAULT 0,
          UNIQUE(log_id, healer_steamid64, patient_steamid64)
        );
        CREATE INDEX IF NOT EXISTS idx_lph_healer ON log_player_healspread(healer_steamid64);
        CREATE INDEX IF NOT EXISTS idx_lph_patient ON log_player_healspread(patient_steamid64);
        CREATE INDEX IF NOT EXISTS idx_lph_log_id ON log_player_healspread(log_id);

        CREATE TABLE IF NOT EXISTS log_rounds (
          id            INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id        INTEGER NOT NULL REFERENCES logs(log_id) ON DELETE CASCADE,
          round_idx     INTEGER NOT NULL,
          duration_secs INTEGER,
          winner        TEXT,
          first_blood_steamid64 TEXT,
          red_kills     INTEGER,
          blue_kills    INTEGER,
          UNIQUE(log_id, round_idx)
        );
        CREATE INDEX IF NOT EXISTS idx_rounds_log_id ON log_rounds(log_id);

        CREATE TABLE IF NOT EXISTS player_names (
          steamid64  TEXT NOT NULL,
          alias      TEXT NOT NULL DEFAULT '',
          log_id     INTEGER NOT NULL,
          date_ts    INTEGER,
          PRIMARY KEY (steamid64, log_id)
        );
        CREATE INDEX IF NOT EXISTS idx_pn_steamid64 ON player_names(steamid64);
        CREATE INDEX IF NOT EXISTS idx_pn_date_ts ON player_names(date_ts);

        CREATE TABLE IF NOT EXISTS player_stats_agg (
          steamid64        TEXT PRIMARY KEY,
          log_count        INTEGER NOT NULL DEFAULT 0,
          wins             INTEGER NOT NULL DEFAULT 0,
          decided_logs     INTEGER NOT NULL DEFAULT 0,
          avg_dpm          REAL,
          avg_kdr          REAL,
          avg_kadr         REAL,
          total_kills      INTEGER NOT NULL DEFAULT 0,
          total_damage     INTEGER NOT NULL DEFAULT 0,
          total_ubers      INTEGER NOT NULL DEFAULT 0,
          total_drops      INTEGER NOT NULL DEFAULT 0,
          updated_at       INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_psa_avg_dpm ON player_stats_agg(avg_dpm DESC);
        CREATE INDEX IF NOT EXISTS idx_psa_avg_kdr ON player_stats_agg(avg_kdr DESC);
        CREATE INDEX IF NOT EXISTS idx_psa_log_count ON player_stats_agg(log_count DESC);
        CREATE INDEX IF NOT EXISTS idx_psa_win_rate ON player_stats_agg(
          (CAST(wins AS REAL) / NULLIF(decided_logs, 0))
        );
        """
    )
    conn.commit()
    _migrate_log_players_imported_at(conn)


def _migrate_log_players_imported_at(conn: sqlite3.Connection) -> None:
    """
    Add ``imported_at`` to ``log_players`` if missing, and keep it aligned with ``logs.imported_at``.

    Runs ``ALTER`` (when needed) and the backfill ``UPDATE`` in one explicit transaction so a
    failed ``UPDATE`` rolls back together with the ``ADD COLUMN`` on SQLite builds where DDL
    participates in the transaction. If a partial state still exists (e.g. older SQLite or an
    interrupted commit), a cheap drift probe on the next run triggers a repair ``UPDATE``.
    """
    cols = {row[1] for row in conn.execute("PRAGMA table_info(log_players)").fetchall()}
    need_column = "imported_at" not in cols

    conn.execute("BEGIN IMMEDIATE")
    try:
        if need_column:
            conn.execute(
                "ALTER TABLE log_players ADD COLUMN imported_at INTEGER NOT NULL DEFAULT 0"
            )

        drift = conn.execute(
            """
            SELECT 1
            FROM log_players lp
            INNER JOIN logs l ON l.log_id = lp.log_id
            WHERE lp.imported_at != l.imported_at
            LIMIT 1
            """
        ).fetchone()

        if drift is not None:
            conn.execute(
                """
                UPDATE log_players
                SET imported_at = COALESCE(
                  (SELECT l.imported_at FROM logs AS l WHERE l.log_id = log_players.log_id),
                  imported_at
                )
                """
            )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def _ubertype_breakdown(stats: dict[str, Any]) -> tuple[int, int, int, int]:
    """Returns (total_ubers, medigun, kritz, other) from ubertypes dict or top-level ubers."""
    ut = stats.get("ubertypes")
    med = kritz = other = 0
    if isinstance(ut, dict):
        for k, v in ut.items():
            kk = str(k).lower()
            n = _int_safe(v, 0)
            if kk in ("medigun", "uber"):
                med += n
            elif kk in ("kritzkrieg", "kritz"):
                kritz += n
            else:
                other += n
        total = med + kritz + other
        if total > 0:
            return total, med, kritz, other
    top = _int_safe(stats.get("ubers"), 0)
    return top, 0, 0, 0


def _heal_spread_dict(stats: dict[str, Any]) -> dict[str, Any]:
    for key in ("heal_spread", "healspread", "heal spread"):
        h = stats.get(key)
        if isinstance(h, dict):
            return h
    return {}


def _weapon_dict(stats: dict[str, Any]) -> dict[str, Any]:
    w = stats.get("weapon") or stats.get("weapons")
    return w if isinstance(w, dict) else {}


def _classkills_dict(stats: dict[str, Any]) -> dict[str, Any]:
    ck = stats.get("classkills") or stats.get("class_kills")
    return ck if isinstance(ck, dict) else {}


def extract_log_stats(log_id: int, logtext: dict[str, Any]) -> dict[str, Any]:
    """Parse one logs.tf JSON dict into row dicts for stats tables."""
    info = logtext.get("info") if isinstance(logtext.get("info"), dict) else {}
    teams = logtext.get("teams")
    names = logtext.get("names")
    players = logtext.get("players")
    if not isinstance(players, dict):
        players = {}

    title = str(info.get("title") or "")
    map_name = str(info.get("map") or "")
    date_ts = info.get("date")
    try:
        date_ts_i = int(date_ts) if date_ts is not None else None
    except (TypeError, ValueError):
        date_ts_i = None

    duration_secs = _int_safe(info.get("total_length"), 0)
    if duration_secs <= 0:
        duration_secs = _int_safe(logtext.get("length"), 0)

    if isinstance(names, dict):
        num_players = len(names)
    else:
        num_players = len(players)

    imported_at = int(time.time())
    winner = _winner_team_from_logtext(logtext)

    log_row = {
        "log_id": log_id,
        "title": title,
        "map": map_name,
        "date_ts": date_ts_i,
        "duration_secs": duration_secs if duration_secs > 0 else None,
        "num_players": num_players,
        "red_score": team_score(teams, "Red"),
        "blue_score": team_score(teams, "Blue"),
        "winner": winner,
        "imported_at": imported_at,
    }

    name_rows: list[dict[str, Any]] = []
    names_dict = logtext.get("names")
    if isinstance(names_dict, dict):
        for steamid3, alias_raw in names_dict.items():
            sid3 = str(steamid3).strip()
            sid64 = steamid3_to_steamid64(sid3)
            if not sid64:
                continue
            alias = str(alias_raw or "").strip()
            if not alias:
                continue
            name_rows.append(
                {
                    "steamid64": sid64,
                    "alias": alias,
                    "log_id": log_id,
                    "date_ts": date_ts_i,
                }
            )

    player_rows: list[dict[str, Any]] = []
    class_rows: list[dict[str, Any]] = []
    weapon_rows: list[dict[str, Any]] = []
    classkill_rows: list[dict[str, Any]] = []
    healspread_rows: list[dict[str, Any]] = []

    for steamid3, stats in players.items():
        if not isinstance(stats, dict):
            continue
        sid3 = str(steamid3).strip()
        sid64 = steamid3_to_steamid64(sid3)
        if not sid64:
            continue

        kills = _int_safe(stats.get("kills"), 0)
        assists = _int_safe(stats.get("assists"), 0)
        deaths = _int_safe(stats.get("deaths"), 0)
        dmg = _int_safe(stats.get("dmg"), 0)
        dmg_taken = _int_safe(stats.get("damage_taken") or stats.get("dmg_taken"), 0)
        # Healing received: long name ``healing_taken`` (some exports) or compact ``hr`` (logs.tf /json).
        # ``heal`` / ``healing`` is healing output (e.g. medic) — do not use for received.
        if "healing_taken" in stats:
            healing_taken = _int_safe(stats.get("healing_taken"), 0)
        else:
            healing_taken = _int_safe(stats.get("hr"), 0)

        u_total, med_u, kritz_u, other_u = _ubertype_breakdown(stats)
        drops = _int_safe(stats.get("drops"), 0)

        hs = _int_safe(stats.get("headshots"), 0)
        # headshots_hit may be 0 while headshots > 0; never use `or` here (0 is falsy).
        hhit_raw = stats.get("headshots_hit")
        hhit = _int_safe(hhit_raw, 0) if hhit_raw is not None else hs
        bs = _int_safe(stats.get("backstabs"), 0)
        # Captures: ``captures`` or compact ``cpc`` (control points) on logs.tf /json.
        if "captures" in stats:
            cap = _int_safe(stats.get("captures"), 0)
        else:
            cap = _int_safe(stats.get("cpc"), 0)
        cap_blk = _int_safe(stats.get("captures_blocked"), 0)
        dom = _int_safe(stats.get("dominated"), 0)
        rev = _int_safe(stats.get("revenges"), 0)
        sui = _int_safe(stats.get("suicides"), 0)
        # Longest killstreak: long name or compact ``lks`` on logs.tf /json.
        if "longest_killstreak" in stats:
            lstreak = _int_safe(stats.get("longest_killstreak"), 0)
        else:
            lstreak = _int_safe(stats.get("lks"), 0)

        raw_dapm = _float_safe(stats.get("dapm"))
        if raw_dapm is not None:
            dapm_val: float | None = round(raw_dapm, 4)
        elif duration_secs > 0:
            dapm_val = round((dmg / float(duration_secs)) * 60.0, 4)
        else:
            dapm_val = None

        if deaths > 0:
            kdr_v = round(kills / float(deaths), 4)
            kadr_v = round((kills + assists) / float(deaths), 4)
        else:
            # No denominator: store NULL for both (consistent for consumers expecting ratio fields).
            kdr_v = None
            kadr_v = None

        primary_class: str | None = None
        class_stats = stats.get("class_stats")
        best_time = -1
        if isinstance(class_stats, list):
            for cs in class_stats:
                if not isinstance(cs, dict):
                    continue
                ct = _int_safe(cs.get("total_time"), 0)
                cname = _normalize_class_name(cs.get("type") or cs.get("class"))
                if cname in _BAD_CLASS_NAMES:
                    continue
                if ct > best_time and cname:
                    best_time = ct
                    primary_class = cname
        if not primary_class and isinstance(class_stats, list):
            for cs in class_stats:
                if isinstance(cs, dict):
                    cname = _normalize_class_name(cs.get("type") or cs.get("class"))
                    if cname and cname not in _BAD_CLASS_NAMES:
                        primary_class = cname
                        break

        team_raw = stats.get("team")
        team = "Red" if team_raw == "Red" else ("Blue" if team_raw == "Blue" else None)

        player_rows.append(
            {
                "log_id": log_id,
                "steamid64": sid64,
                "steamid3": sid3,
                "team": team,
                "kills": kills,
                "assists": assists,
                "deaths": deaths,
                "damage": dmg,
                "damage_taken": dmg_taken,
                "healing_taken": healing_taken,
                "ubers": u_total,
                "drops": drops,
                "medigun_ubers": med_u,
                "kritz_ubers": kritz_u,
                "other_ubers": other_u,
                "headshots": hs,
                "headshots_hit": hhit,
                "backstabs": bs,
                "captures": cap,
                "captures_blocked": cap_blk,
                "dominated": dom,
                "revenges": rev,
                "suicides": sui,
                "longest_killstreak": lstreak,
                "dapm": dapm_val,
                "kdr": kdr_v,
                "kadr": kadr_v,
                "primary_class": primary_class,
                "imported_at": imported_at,
            }
        )

        if isinstance(class_stats, list):
            for cs in class_stats:
                if not isinstance(cs, dict):
                    continue
                cname = _normalize_class_name(cs.get("type") or cs.get("class"))
                if not cname or cname in _BAD_CLASS_NAMES:
                    continue
                class_rows.append(
                    {
                        "log_id": log_id,
                        "steamid64": sid64,
                        "class": cname,
                        "playtime": _int_safe(cs.get("total_time"), 0),
                        "kills": _int_safe(cs.get("kills"), 0),
                        "assists": _int_safe(cs.get("assists"), 0),
                        "deaths": _int_safe(cs.get("deaths"), 0),
                        "damage": _int_safe(cs.get("dmg") or cs.get("damage"), 0),
                    }
                )

        for wname, wst in _weapon_dict(stats).items():
            wn = str(wname).strip()
            if not wn or not isinstance(wst, dict):
                continue
            avg_d = _float_safe(wst.get("avg_dmg") or wst.get("avg_damage"))
            weapon_rows.append(
                {
                    "log_id": log_id,
                    "steamid64": sid64,
                    "weapon": wn,
                    "kills": _int_safe(wst.get("kills"), 0),
                    "damage": _int_safe(wst.get("dmg") or wst.get("damage"), 0),
                    "avg_damage": avg_d,
                    "shots": _int_safe(wst.get("shots"), 0),
                    "hits": _int_safe(wst.get("hits"), 0),
                }
            )

        for victim, kc in _classkills_dict(stats).items():
            vc = str(victim).strip().lower()
            if not vc or vc in _BAD_CLASS_NAMES:
                continue
            classkill_rows.append(
                {
                    "log_id": log_id,
                    "steamid64": sid64,
                    "victim_class": vc,
                    "kills": _int_safe(kc, 0),
                }
            )

        for patient3, heal_amt in _heal_spread_dict(stats).items():
            p64 = steamid3_to_steamid64(str(patient3).strip())
            if not p64:
                continue
            healspread_rows.append(
                {
                    "log_id": log_id,
                    "healer_steamid64": sid64,
                    "patient_steamid64": p64,
                    "healing": _int_safe(heal_amt, 0),
                }
            )

    round_rows: list[dict[str, Any]] = []
    rounds = logtext.get("rounds")
    if isinstance(rounds, list):
        for idx, rnd in enumerate(rounds):
            if not isinstance(rnd, dict):
                continue
            dur_i = _round_duration_secs_from_log(rnd)
            rw = rnd.get("winner")
            rw_s: str | None = None
            if rw == "Red":
                rw_s = "Red"
            elif rw == "Blue":
                rw_s = "Blue"
            elif isinstance(rw, str):
                rl = rw.strip().lower()
                if rl == "red":
                    rw_s = "Red"
                elif rl in ("blue", "blu"):
                    rw_s = "Blue"

            kills_blk = rnd.get("kills")
            rk: int | None = None
            bk: int | None = None
            if isinstance(kills_blk, dict):
                if "Red" in kills_blk:
                    rk = _int_safe(kills_blk.get("Red"), 0)
                elif "red" in kills_blk:
                    rk = _int_safe(kills_blk.get("red"), 0)
                if "Blue" in kills_blk:
                    bk = _int_safe(kills_blk.get("Blue"), 0)
                elif "blue" in kills_blk:
                    bk = _int_safe(kills_blk.get("blue"), 0)

            fb64 = _first_blood_steamid64_from_round(rnd)

            round_rows.append(
                {
                    "log_id": log_id,
                    "round_idx": idx,
                    "duration_secs": dur_i,
                    "winner": rw_s,
                    "first_blood_steamid64": fb64,
                    "red_kills": rk,
                    "blue_kills": bk,
                }
            )

    return {
        "log_row": log_row,
        "player_rows": player_rows,
        "class_rows": class_rows,
        "weapon_rows": weapon_rows,
        "classkill_rows": classkill_rows,
        "healspread_rows": healspread_rows,
        "round_rows": round_rows,
        "name_rows": name_rows,
    }


def replace_stats_for_log(conn: sqlite3.Connection, log_id: int, logtext: dict[str, Any]) -> int:
    """
    Replace all stats rows for one log atomically. Caller controls transaction.
    Returns number of player rows inserted.

    Does not update ``player_stats_agg``; callers that need leaderboard aggregates should
    collect affected SteamID64s and call ``flush_player_stats_agg`` after a batch of writes.
    """
    conn.execute("DELETE FROM logs WHERE log_id = ?", (log_id,))
    data = extract_log_stats(log_id, logtext)
    lr = data["log_row"]
    conn.execute(
        """
        INSERT INTO logs (log_id, title, map, date_ts, duration_secs, num_players, red_score, blue_score, winner, imported_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            lr["log_id"],
            lr["title"],
            lr["map"],
            lr["date_ts"],
            lr["duration_secs"],
            lr["num_players"],
            lr["red_score"],
            lr["blue_score"],
            lr["winner"],
            lr["imported_at"],
        ),
    )

    pr = data["player_rows"]
    if pr:
        conn.executemany(
            """
            INSERT INTO log_players (
              log_id, steamid64, steamid3, team,
              kills, assists, deaths, damage, damage_taken, healing_taken,
              ubers, drops, medigun_ubers, kritz_ubers, other_ubers,
              headshots, headshots_hit, backstabs, captures, captures_blocked,
              dominated, revenges, suicides, longest_killstreak,
              dapm, kdr, kadr, primary_class, imported_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    r["log_id"],
                    r["steamid64"],
                    r["steamid3"],
                    r["team"],
                    r["kills"],
                    r["assists"],
                    r["deaths"],
                    r["damage"],
                    r["damage_taken"],
                    r["healing_taken"],
                    r["ubers"],
                    r["drops"],
                    r["medigun_ubers"],
                    r["kritz_ubers"],
                    r["other_ubers"],
                    r["headshots"],
                    r["headshots_hit"],
                    r["backstabs"],
                    r["captures"],
                    r["captures_blocked"],
                    r["dominated"],
                    r["revenges"],
                    r["suicides"],
                    r["longest_killstreak"],
                    r["dapm"],
                    r["kdr"],
                    r["kadr"],
                    r["primary_class"],
                    r["imported_at"],
                )
                for r in pr
            ],
        )

    cr = data["class_rows"]
    if cr:
        conn.executemany(
            """
            INSERT INTO log_player_classes (log_id, steamid64, class, playtime, kills, assists, deaths, damage)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    r["log_id"],
                    r["steamid64"],
                    r["class"],
                    r["playtime"],
                    r["kills"],
                    r["assists"],
                    r["deaths"],
                    r["damage"],
                )
                for r in cr
            ],
        )

    wr = data["weapon_rows"]
    if wr:
        conn.executemany(
            """
            INSERT INTO log_player_weapons (log_id, steamid64, weapon, kills, damage, avg_damage, shots, hits)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    r["log_id"],
                    r["steamid64"],
                    r["weapon"],
                    r["kills"],
                    r["damage"],
                    r["avg_damage"],
                    r["shots"],
                    r["hits"],
                )
                for r in wr
            ],
        )

    ckr = data["classkill_rows"]
    if ckr:
        conn.executemany(
            """
            INSERT INTO log_player_classkills (log_id, steamid64, victim_class, kills)
            VALUES (?, ?, ?, ?)
            """,
            [(r["log_id"], r["steamid64"], r["victim_class"], r["kills"]) for r in ckr],
        )

    hr = data["healspread_rows"]
    if hr:
        conn.executemany(
            """
            INSERT INTO log_player_healspread (log_id, healer_steamid64, patient_steamid64, healing)
            VALUES (?, ?, ?, ?)
            """,
            [
                (r["log_id"], r["healer_steamid64"], r["patient_steamid64"], r["healing"])
                for r in hr
            ],
        )

    rr = data["round_rows"]
    if rr:
        conn.executemany(
            """
            INSERT INTO log_rounds (log_id, round_idx, duration_secs, winner, first_blood_steamid64, red_kills, blue_kills)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    r["log_id"],
                    r["round_idx"],
                    r["duration_secs"],
                    r["winner"],
                    r["first_blood_steamid64"],
                    r["red_kills"],
                    r["blue_kills"],
                )
                for r in rr
            ],
        )

    nr = data["name_rows"]
    if nr:
        conn.executemany(
            """
            INSERT OR REPLACE INTO player_names (steamid64, alias, log_id, date_ts)
            VALUES (?, ?, ?, ?)
            """,
            [(r["steamid64"], r["alias"], r["log_id"], r["date_ts"]) for r in nr],
        )

    return len(pr)


def rebuild_player_stats_agg(conn: sqlite3.Connection) -> int:
    """
    Full rebuild of ``player_stats_agg`` from ``log_players`` + ``logs`` (global unfiltered aggregates).
    Run after schema upgrades or via ``python -m app.rebuild_agg``.
    """
    ts = int(time.time())
    with conn:
        conn.execute("DELETE FROM player_stats_agg")
        conn.execute(
            """
            INSERT INTO player_stats_agg (
              steamid64, log_count, wins, decided_logs, avg_dpm, avg_kdr, avg_kadr,
              total_kills, total_damage, total_ubers, total_drops, updated_at
            )
            SELECT
              lp.steamid64,
              COUNT(*),
              SUM(CASE WHEN l.winner IS NOT NULL AND l.winner = lp.team THEN 1 ELSE 0 END),
              SUM(CASE WHEN l.winner IS NOT NULL THEN 1 ELSE 0 END),
              AVG(CASE WHEN lp.dapm IS NOT NULL THEN lp.dapm END),
              AVG(CASE WHEN lp.kdr IS NOT NULL THEN lp.kdr END),
              AVG(CASE WHEN lp.kadr IS NOT NULL THEN lp.kadr END),
              SUM(lp.kills),
              SUM(lp.damage),
              SUM(lp.ubers),
              SUM(lp.drops),
              ?
            FROM logs l
            INNER JOIN log_players lp ON lp.log_id = l.log_id AND lp.team IN ('Red', 'Blue')
            GROUP BY lp.steamid64
            """,
            (ts,),
        )
    row = conn.execute("SELECT COUNT(*) FROM player_stats_agg").fetchone()
    return int(row[0] or 0) if row else 0


def refresh_player_stats_agg_for_steamids(conn: sqlite3.Connection, steamids: Sequence[str]) -> None:
    """
    Recompute aggregate rows for the given SteamID64s (after a batch of log writes).
    Uses batched ``WHERE steamid64 IN (...)`` queries (chunked at ``_PSA_CHUNK``) instead of one query per player.

    All chunk updates run inside a single ``SAVEPOINT`` so the batch is atomic: on failure, no partial
    leaderboard rows remain from this call (SQLite rolls back to the savepoint).
    """
    uniq = list(dict.fromkeys(s.strip() for s in steamids if s and str(s).strip()))
    if not uniq:
        return
    ts = int(time.time())
    conn.execute(f"SAVEPOINT {_PSA_SAVEPOINT}")
    try:
        _refresh_player_stats_agg_for_steamids_impl(conn, uniq, ts)
        conn.execute(f"RELEASE SAVEPOINT {_PSA_SAVEPOINT}")
    except Exception:
        conn.execute(f"ROLLBACK TO SAVEPOINT {_PSA_SAVEPOINT}")
        conn.execute(f"RELEASE SAVEPOINT {_PSA_SAVEPOINT}")
        raise


def _refresh_player_stats_agg_for_steamids_impl(
    conn: sqlite3.Connection,
    uniq: list[str],
    ts: int,
) -> None:
    sel_prefix = """
        SELECT
          lp.steamid64,
          COUNT(*),
          SUM(CASE WHEN l.winner IS NOT NULL AND l.winner = lp.team THEN 1 ELSE 0 END),
          SUM(CASE WHEN l.winner IS NOT NULL THEN 1 ELSE 0 END),
          AVG(CASE WHEN lp.dapm IS NOT NULL THEN lp.dapm END),
          AVG(CASE WHEN lp.kdr IS NOT NULL THEN lp.kdr END),
          AVG(CASE WHEN lp.kadr IS NOT NULL THEN lp.kadr END),
          SUM(lp.kills),
          SUM(lp.damage),
          SUM(lp.ubers),
          SUM(lp.drops)
        FROM logs l
        INNER JOIN log_players lp ON lp.log_id = l.log_id AND lp.team IN ('Red', 'Blue')
        WHERE lp.steamid64 IN (
    """
    sel_suffix = """
        )
        GROUP BY lp.steamid64
    """
    upsert = """
        INSERT OR REPLACE INTO player_stats_agg (
          steamid64, log_count, wins, decided_logs, avg_dpm, avg_kdr, avg_kadr,
          total_kills, total_damage, total_ubers, total_drops, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    for i in range(0, len(uniq), _PSA_CHUNK):
        chunk = uniq[i : i + _PSA_CHUNK]
        placeholders = ",".join("?" * len(chunk))
        sql = sel_prefix + placeholders + sel_suffix
        rows = conn.execute(sql, chunk).fetchall()
        seen: set[str] = set()
        for row in rows:
            if not row or not row[0]:
                continue
            sid = str(row[0]).strip()
            if not sid:
                continue
            seen.add(sid)
            if int(row[1] or 0) == 0:
                conn.execute("DELETE FROM player_stats_agg WHERE steamid64 = ?", (sid,))
                continue
            conn.execute(
                upsert,
                (
                    sid,
                    int(row[1] or 0),
                    int(row[2] or 0),
                    int(row[3] or 0),
                    row[4],
                    row[5],
                    row[6],
                    int(row[7] or 0),
                    int(row[8] or 0),
                    int(row[9] or 0),
                    int(row[10] or 0),
                    ts,
                ),
            )
        for sid in chunk:
            if sid not in seen:
                conn.execute("DELETE FROM player_stats_agg WHERE steamid64 = ?", (sid,))


def flush_player_stats_agg(
    conn: sqlite3.Connection,
    pending_steamids: set[str],
) -> int:
    """
    Refresh ``player_stats_agg`` for all SteamID64s in ``pending_steamids``, then clear the set.
    Returns the number of SteamID64s processed.
    Call once after a batch of ``replace_stats_for_log`` calls rather than after each one.

    The underlying refresh is atomic (see ``refresh_player_stats_agg_for_steamids``).
    """
    if not pending_steamids:
        return 0
    ids = list(pending_steamids)
    try:
        refresh_player_stats_agg_for_steamids(conn, ids)
    except Exception:
        logger.exception("flush_player_stats_agg failed for %d player(s)", len(ids))
        return 0
    pending_steamids.clear()
    return len(ids)


def player_stats_agg_nonempty(db_path: str | Path) -> bool:
    """True if ``player_stats_agg`` has at least one row (fast leaderboard path available)."""
    path = Path(db_path)
    if not path.is_file():
        return False
    try:
        conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10.0)
        try:
            conn.execute("PRAGMA busy_timeout=10000")
            n = conn.execute("SELECT 1 FROM player_stats_agg LIMIT 1").fetchone()
            return n is not None
        finally:
            conn.close()
    except Exception:
        return False


def stats_db_fingerprint(db_path: str | Path) -> frozenset[int]:
    """
    Lightweight fingerprint for stats DB contents.

    Encodes (logs row count, max log_id, max imported_at) so cache invalidates when a log is
    re-imported (same count / max id) or when ``player_stats_agg``-relevant data changes.
    """
    path = Path(db_path)
    if not path.is_file():
        return frozenset()
    try:
        conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10.0)
        try:
            conn.execute("PRAGMA busy_timeout=10000")
            row = conn.execute(
                "SELECT COUNT(*), COALESCE(MAX(log_id), 0), COALESCE(MAX(imported_at), 0) FROM logs"
            ).fetchone()
        finally:
            conn.close()
    except Exception:
        return frozenset()
    count = int(row[0] or 0) if row else 0
    max_id = int(row[1] or 0) if row else 0
    max_imp = int(row[2] or 0) if row else 0
    return frozenset((count, max_id, max_imp))


def stats_player_stats_cache_token(db_path: str | Path, steamid64: str) -> frozenset[int]:
    """
    Small fingerprint for per-player stats/coplayers/profile cache validation.
    Avoids loading every ``log_id`` for the player on each cache hit.

    Uses ``log_players.imported_at`` (denormalized from ``logs`` at insert) so validation is a
    single index lookup on ``steamid64`` with no join.
    """
    path = Path(db_path)
    sid = (steamid64 or "").strip()
    if not path.is_file() or not sid:
        return frozenset()
    try:
        conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10.0)
        try:
            conn.execute("PRAGMA busy_timeout=10000")
            try:
                row = conn.execute(
                    """
                    SELECT COUNT(*), COALESCE(MAX(log_id), 0), COALESCE(SUM(imported_at), 0)
                    FROM log_players
                    WHERE steamid64 = ?
                    """,
                    (sid,),
                ).fetchone()
            except sqlite3.OperationalError:
                # Pre-migration DB (column not added yet) or very old file — join ``logs``.
                row = conn.execute(
                    """
                    SELECT COUNT(lp.log_id), COALESCE(MAX(lp.log_id), 0), COALESCE(SUM(l.imported_at), 0)
                    FROM log_players lp
                    INNER JOIN logs l ON l.log_id = lp.log_id
                    WHERE lp.steamid64 = ?
                    """,
                    (sid,),
                ).fetchone()
        finally:
            conn.close()
    except Exception:
        return frozenset()
    if not row:
        return frozenset()
    cnt = int(row[0] or 0)
    mx = int(row[1] or 0)
    s = int(row[2] or 0)
    return frozenset((cnt, mx, s))


def stats_log_ids_for_player(db_path: str | Path, steamid64: str) -> frozenset[int]:
    """Return frozenset of log_ids in stats DB for this player. Empty if DB unavailable."""
    path = Path(db_path)
    if not path.is_file():
        return frozenset()
    try:
        conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10.0)
        try:
            conn.execute("PRAGMA busy_timeout=10000")
            rows = conn.execute(
                "SELECT log_id FROM log_players WHERE steamid64 = ?", (steamid64,)
            ).fetchall()
            return frozenset(int(r[0]) for r in rows if r and r[0] is not None)
        finally:
            conn.close()
    except Exception:
        return frozenset()


def lookup_player_names(
    db_path: str | Path,
    steamid64s: list[str],
) -> dict[str, str]:
    """
    Most recent known alias for each steamid64 from player_names table.
    Returns {steamid64: alias}. Missing = no name found.
    Chunks queries to stay under SQLite variable limit.
    """
    path = Path(db_path)
    if not path.is_file() or not steamid64s:
        return {}
    uniq: list[str] = list(dict.fromkeys(s.strip() for s in steamid64s if s and s.strip()))
    if not uniq:
        return {}
    out: dict[str, str] = {}
    _max_vars = 900
    try:
        conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10.0)
        try:
            conn.execute("PRAGMA busy_timeout=10000")
            for i in range(0, len(uniq), _max_vars):
                batch = uniq[i : i + _max_vars]
                ph = ",".join("?" * len(batch))
                sql = f"""
                    SELECT steamid64, alias
                    FROM (
                        SELECT steamid64, alias,
                               ROW_NUMBER() OVER (
                                 PARTITION BY steamid64
                                 ORDER BY COALESCE(date_ts, 0) DESC, log_id DESC
                               ) AS rn
                        FROM player_names
                        WHERE steamid64 IN ({ph})
                          AND COALESCE(TRIM(alias), '') != ''
                    )
                    WHERE rn = 1
                """
                for sid64, alias in conn.execute(sql, batch).fetchall():
                    sid_s = str(sid64).strip()
                    alias_s = str(alias or "").strip()
                    if sid_s and alias_s:
                        out[sid_s] = alias_s
        finally:
            conn.close()
    except Exception:
        return out
    return out
