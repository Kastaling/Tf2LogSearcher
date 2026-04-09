"""SQLite storage for per-log player stats from logs.tf JSON (downloader + backfill)."""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any

from app.logs_tf import steamid3_to_steamid64


def connect_stats_db(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


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


def _team_score(teams: Any, key: str) -> int | None:
    if not isinstance(teams, dict):
        return None
    block = teams.get(key)
    if not isinstance(block, dict):
        return None
    raw = block.get("score")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _winner_from_info_field(w: Any) -> str | None:
    if w is None:
        return None
    if isinstance(w, str):
        s = w.strip()
        if not s:
            return None
        if s in ("Red", "Blue"):
            return s
        low = s.casefold()
        if low == "red":
            return "Red"
        if low in ("blue", "blu"):
            return "Blue"
    return None


def _winner_team_from_logtext(logtext: dict[str, Any]) -> str | None:
    info = logtext.get("info")
    if isinstance(info, dict):
        parsed = _winner_from_info_field(info.get("winner"))
        if parsed is not None:
            return parsed
    teams = logtext.get("teams")
    rs = _team_score(teams, "Red")
    bs = _team_score(teams, "Blue")
    if rs is None or bs is None:
        return None
    if rs > bs:
        return "Red"
    if bs > rs:
        return "Blue"
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
          UNIQUE(log_id, steamid64)
        );
        CREATE INDEX IF NOT EXISTS idx_log_players_steamid64 ON log_players(steamid64);
        CREATE INDEX IF NOT EXISTS idx_log_players_log_id ON log_players(log_id);
        CREATE INDEX IF NOT EXISTS idx_log_players_steamid64_log_id ON log_players(steamid64, log_id);

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
        """
    )
    conn.commit()


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
        "red_score": _team_score(teams, "Red"),
        "blue_score": _team_score(teams, "Blue"),
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
        # "healing" in logs.tf is heals output (e.g. medic); do not use it for healing received.
        healing_taken = _int_safe(stats.get("healing_taken"), 0)

        u_total, med_u, kritz_u, other_u = _ubertype_breakdown(stats)
        drops = _int_safe(stats.get("drops"), 0)

        hs = _int_safe(stats.get("headshots"), 0)
        # headshots_hit may be 0 while headshots > 0; never use `or` here (0 is falsy).
        hhit_raw = stats.get("headshots_hit")
        hhit = _int_safe(hhit_raw, 0) if hhit_raw is not None else hs
        bs = _int_safe(stats.get("backstabs"), 0)
        cap = _int_safe(stats.get("captures"), 0)
        cap_blk = _int_safe(stats.get("captures_blocked"), 0)
        dom = _int_safe(stats.get("dominated"), 0)
        rev = _int_safe(stats.get("revenges"), 0)
        sui = _int_safe(stats.get("suicides"), 0)
        lstreak = _int_safe(stats.get("longest_killstreak"), 0)

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
                cname = str(cs.get("type") or cs.get("class") or "").strip().lower()
                if ct > best_time and cname:
                    best_time = ct
                    primary_class = cname
        if not primary_class and isinstance(class_stats, list):
            for cs in class_stats:
                if isinstance(cs, dict):
                    cname = str(cs.get("type") or cs.get("class") or "").strip().lower()
                    if cname:
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
            }
        )

        if isinstance(class_stats, list):
            for cs in class_stats:
                if not isinstance(cs, dict):
                    continue
                cname = str(cs.get("type") or cs.get("class") or "").strip().lower()
                if not cname:
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
            if not vc:
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
            dur = rnd.get("duration")
            try:
                dur_i = int(dur) if dur is not None else None
            except (TypeError, ValueError):
                dur_i = None
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

            fc = rnd.get("firstcap") or rnd.get("first_blood") or rnd.get("firstblood")
            fb64: str | None = None
            if fc is not None:
                fb64 = steamid3_to_steamid64(str(fc).strip())

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
              dapm, kdr, kadr, primary_class
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
