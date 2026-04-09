"""Search logic: chat search, stats search, log match. Pure Python, no HTTP."""
import json
import logging
import re
import sqlite3
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from app.chat_db import CHAT_ALIAS_FTS_READY_META_KEY
from app.config import CHAT_DB_PATH, STATS_DB_PATH
from app.log_utils import winner_team_from_log as _winner_team_from_log
from app.stats_db import stats_log_ids_for_player
from app.logs_tf import get_log_list_for_player, steamid3_to_steamid64, steamid64_to_steamid3

logger = logging.getLogger(__name__)

LOGS_TF_URL_BASE = "https://logs.tf"

# SQLite bind parameter limit (max 999); stay under with margin for IN lists.
_SQLITE_MAX_VARS = 900

# DB-backed co-players: max rows returned (ORDER BY total games desc). Prevents huge responses
# and multi-batch chat alias lookups that could stall the worker on very active accounts.
_COPLAYERS_DB_RESULT_LIMIT = 5000

# logs.tf player class_stats "type" values we expose to the API (whitelist).
_LOGMATCH_CLASS_TYPES: frozenset[str] = frozenset({
    "scout",
    "soldier",
    "pyro",
    "demoman",
    "heavyweapons",
    "engineer",
    "medic",
    "sniper",
    "spy",
})


def _class_playtime_for_logmatch(stats: dict[str, Any]) -> list[dict[str, Any]]:
    """Per-class playtime in seconds from logs.tf class_stats (longest first)."""
    raw = stats.get("class_stats")
    if not isinstance(raw, list):
        return []
    pairs: list[tuple[str, int]] = []
    for cs in raw:
        if not isinstance(cs, dict):
            continue
        ctype = cs.get("type")
        if not isinstance(ctype, str) or ctype not in _LOGMATCH_CLASS_TYPES:
            continue
        try:
            sec = int(cs.get("total_time") or 0)
        except (TypeError, ValueError):
            continue
        if sec <= 0:
            continue
        pairs.append((ctype, sec))
    pairs.sort(key=lambda x: -x[1])
    return [{"class": a, "seconds": b} for a, b in pairs]


# Limits to prevent runaway queries and huge responses
CHAT_SEARCH_MAX_RESULTS_WITH_STEAMID = 5000   # when showing one player's chat (with or without word filter)
CHAT_CONTEXT_PREVIEW_MAX_CHARS = 220
CHAT_SEARCH_LEADERBOARD_MAX_ROWS = 500


def local_log_ids_for_player(steamid64: str, logs_dir: str | Path) -> frozenset[int]:
    """Set of log IDs we have locally for this player (intersection of API list and existing files)."""
    logs_dir = Path(logs_dir)
    log_ids = get_log_list_for_player(steamid64)
    return frozenset(lid for lid in log_ids if (logs_dir / f"{lid}.json").exists())


def _player_count_filter(player_count: int, gamemode: str) -> bool:
    """True if player count matches gamemode (hl, 7s, 6s, ud)."""
    if gamemode == "hl":
        return player_count >= 18
    if gamemode == "7s":
        return 14 <= player_count <= 17
    if gamemode == "6s":
        return 12 <= player_count <= 13
    if gamemode == "ud":
        return 4 <= player_count <= 6
    return False


def _log_in_date_range(
    log_ts: Any,
    date_from: date | None,
    date_to: date | None,
) -> bool:
    """True when log timestamp is within [date_from, date_to] (UTC calendar date, inclusive)."""
    if date_from is None and date_to is None:
        return True
    try:
        ts = int(log_ts or 0)
    except (TypeError, ValueError):
        return False
    if ts <= 0:
        return False
    d = datetime.fromtimestamp(ts, tz=timezone.utc).date()
    if date_from is not None and d < date_from:
        return False
    if date_to is not None and d > date_to:
        return False
    return True


def _map_matches_query(map_name: Any, map_query: str | None) -> bool:
    """True when map name contains query (case-insensitive); empty query means no filter."""
    q = (map_query or "").strip().lower()
    if not q:
        return True
    m = str(map_name or "").strip().lower()
    if not m:
        return False
    return q in m


def _date_range_to_unix_bounds(
    date_from: date | None,
    date_to: date | None,
) -> tuple[int | None, int | None]:
    """Inclusive UTC second bounds for [date_from, date_to]."""
    start_ts: int | None = None
    end_ts: int | None = None
    if date_from is not None:
        start_dt = datetime(date_from.year, date_from.month, date_from.day, tzinfo=timezone.utc)
        start_ts = int(start_dt.timestamp())
    if date_to is not None:
        # End of day inclusive.
        end_dt = datetime(date_to.year, date_to.month, date_to.day, 23, 59, 59, tzinfo=timezone.utc)
        end_ts = int(end_dt.timestamp())
    return start_ts, end_ts


def _sqlite_connect_ro(path: Path) -> sqlite3.Connection:
    """Open any SQLite file at ``path`` read-only (stats DB, chat DB, etc.); not tied to one schema."""
    conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10.0)
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def _stats_db_available_for_player(steamid64: str) -> bool:
    path = Path(STATS_DB_PATH)
    if not path.is_file():
        return False
    sid = (steamid64 or "").strip()
    if not sid:
        return False
    try:
        conn = _sqlite_connect_ro(path)
        try:
            row = conn.execute(
                "SELECT 1 FROM log_players WHERE steamid64 = ? LIMIT 1",
                (sid,),
            ).fetchone()
            return row is not None
        finally:
            conn.close()
    except Exception:
        return False


def _gamemode_sql_stats(gamemode: str) -> tuple[str, list[Any]]:
    """Optional AND clause for logs.num_players (stats DB path). Empty/other = no filter."""
    gm = (gamemode or "").strip()
    if gm == "hl":
        return " AND l.num_players >= ?", [18]
    if gm == "7s":
        return " AND l.num_players BETWEEN ? AND ?", [14, 17]
    if gm == "6s":
        return " AND l.num_players BETWEEN ? AND ?", [12, 13]
    if gm == "ud":
        return " AND l.num_players BETWEEN ? AND ?", [4, 6]
    return "", []


def _gamemode_sql_coplayers(gamemode: str) -> tuple[str, list[Any]]:
    """Same numeric ranges as file coplayers_search when gamemode is hl/7s/6s/ud; else no filter."""
    gm = (gamemode or "").strip()
    if gm not in ("hl", "7s", "6s", "ud"):
        return "", []
    return _gamemode_sql_stats(gm)


def _sql_float(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _lookup_from_chat_db_only(steamid64s: list[str]) -> dict[str, str]:
    """Most recent non-empty alias per steamid64 from chat_messages only (ROW_NUMBER)."""
    path = Path(CHAT_DB_PATH)
    if not path.is_file():
        return {}
    uniq: list[str] = []
    seen: set[str] = set()
    for s in steamid64s:
        t = (s or "").strip()
        if t and t not in seen:
            seen.add(t)
            uniq.append(t)
    if not uniq:
        return {}
    out: dict[str, str] = {}
    try:
        conn = _sqlite_connect_ro(path)
        try:
            for i in range(0, len(uniq), _SQLITE_MAX_VARS):
                batch = uniq[i : i + _SQLITE_MAX_VARS]
                ph = ",".join("?" * len(batch))
                sql = f"""
                    SELECT steamid64, alias
                    FROM (
                        SELECT steamid64, alias,
                               ROW_NUMBER() OVER (
                                 PARTITION BY steamid64 ORDER BY log_id DESC, id DESC
                               ) AS rn
                        FROM chat_messages
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
        return {}
    return out


def _lookup_aliases_from_chat_db(steamid64s: list[str]) -> dict[str, str]:
    """
    Most recent alias per steamid64: stats DB ``player_names`` first (log roster names),
    then chat_messages for any still missing.
    """
    if not steamid64s:
        return {}
    from app.stats_db import lookup_player_names

    out = lookup_player_names(STATS_DB_PATH, steamid64s)
    uniq = list(dict.fromkeys((s or "").strip() for s in steamid64s if (s or "").strip()))
    missing = [s for s in uniq if s not in out]
    if missing:
        out.update(_lookup_from_chat_db_only(missing))
    return out


def _ctx_from_db_row(name: Any, msg: Any, team: Any) -> dict[str, Any] | None:
    """Context payload matching existing chat API shape."""
    t = str(msg or "").strip()
    if not t:
        return None
    if len(t) > CHAT_CONTEXT_PREVIEW_MAX_CHARS:
        t = t[: CHAT_CONTEXT_PREVIEW_MAX_CHARS - 1] + "…"
    n = str(name or "").strip()
    tm = "Red" if team == "Red" else ("Blue" if team == "Blue" else None)
    return {"name": n, "msg": t, "team": tm}


def _sqlite_has_chat_fts(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='chat_messages_fts' LIMIT 1"
    ).fetchone()
    return bool(row)


def _fts_phrase_query(raw: str) -> str:
    """Literal phrase query for FTS5 MATCH (double quotes escaped)."""
    s = (raw or "").strip()
    if not s:
        return ""
    # Keep alnum / underscore terms to avoid malformed MATCH syntax.
    cleaned = re.sub(r"\s+", " ", s)
    cleaned = cleaned.replace('"', '""').strip()
    return f"\"{cleaned}\""


def chat_search_sqlite(
    word: str,
    steamid: str,
    db_path: str | Path,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    map_query: str = "",
) -> tuple[list[dict[str, Any]], int, str | None, frozenset[int]]:
    """
    Search chat in SQLite DB. Same return shape as ``chat_search``.

    ``log_ids_used`` is the set of log IDs where this player has chat rows in DB.
    """
    word = (word or "").strip()
    steamid = (steamid or "").strip()
    has_word = bool(word)
    word_lower = word.lower() if has_word else ""
    steamid3 = steamid64_to_steamid3(steamid)
    map_q = (map_query or "").strip().lower()
    start_ts, end_ts = _date_range_to_unix_bounds(date_from, date_to)

    base_select = """
        SELECT
          cm.log_id,
          cm.alias,
          cm.msg,
          cm.team,
          p.alias AS prev_alias,
          p.msg AS prev_msg,
          p.team AS prev_team,
          n.alias AS next_alias,
          n.msg AS next_msg,
          n.team AS next_team
        FROM chat_messages AS cm
        JOIN chat_logs AS cl ON cl.log_id = cm.log_id
        LEFT JOIN chat_messages AS p
          ON p.log_id = cm.log_id
         AND p.message_idx = cm.message_idx - 1
        LEFT JOIN chat_messages AS n
          ON n.log_id = cm.log_id
         AND n.message_idx = cm.message_idx + 1
    """

    where_tail = """
        WHERE cm.steamid3 = ?
          AND (? IS NULL OR cl.log_date_ts >= ?)
          AND (? IS NULL OR cl.log_date_ts <= ?)
          AND (? = '' OR instr(lower(cl.map), ?) > 0)
        ORDER BY cm.log_id DESC, cm.message_idx ASC
        LIMIT ?
    """

    conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        has_fts = has_word and _sqlite_has_chat_fts(conn)
        if has_fts:
            fts_q = _fts_phrase_query(word)
            sql = (
                base_select
                + """
                JOIN chat_messages_fts AS fts ON fts.rowid = cm.id
                """
                + where_tail.replace("WHERE cm.steamid3 = ?", "WHERE cm.steamid3 = ? AND fts.msg MATCH ?")
            )
            params: tuple[Any, ...] = (
                steamid3,
                fts_q,
                start_ts,
                start_ts,
                end_ts,
                end_ts,
                map_q,
                map_q,
                CHAT_SEARCH_MAX_RESULTS_WITH_STEAMID,
            )
        else:
            sql = (
                base_select
                + where_tail.replace(
                    "WHERE cm.steamid3 = ?",
                    "WHERE cm.steamid3 = ? AND (? = '' OR instr(lower(cm.msg), ?) > 0)",
                )
            )
            params = (
                steamid3,
                word_lower,
                word_lower,
                start_ts,
                start_ts,
                end_ts,
                end_ts,
                map_q,
                map_q,
                CHAT_SEARCH_MAX_RESULTS_WITH_STEAMID,
            )
        rows = conn.execute(sql, params).fetchall()
        log_rows = conn.execute(
            "SELECT DISTINCT log_id FROM chat_messages WHERE steamid3 = ?",
            (steamid3,),
        ).fetchall()
    finally:
        conn.close()

    results: list[dict[str, Any]] = []
    for r in rows:
        log_id = int(r[0])
        alias = str(r[1] or "")
        msg = str(r[2] or "")
        team = "Red" if r[3] == "Red" else ("Blue" if r[3] == "Blue" else None)
        results.append(
            {
                "log_id": log_id,
                "alias": alias,
                "msg": msg,
                "context_prev": _ctx_from_db_row(r[4], r[5], r[6]),
                "context_next": _ctx_from_db_row(r[7], r[8], r[9]),
                "url": f"{LOGS_TF_URL_BASE}/{log_id}",
                "team": team,
            }
        )
    searched_name = results[0]["alias"].strip() if results else None
    if searched_name == "":
        searched_name = None
    log_ids_used = frozenset(int(x[0]) for x in log_rows if x and x[0] is not None)
    return results, len(results), searched_name, log_ids_used


def chat_leaderboard_search_sqlite(
    word: str,
    db_path: str | Path,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    map_query: str = "",
) -> tuple[list[dict[str, Any]], int, int]:
    """
    Global word leaderboard in chat DB (no SteamID filter).

    Returns (rows, total_rows, logs_searched_for_match_set).
    """
    q = (word or "").strip()
    q_lower = q.lower()
    mq = (map_query or "").strip().lower()
    start_ts, end_ts = _date_range_to_unix_bounds(date_from, date_to)

    conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        has_fts = _sqlite_has_chat_fts(conn)
        if has_fts:
            where_filter = "fts.msg MATCH ?"
            query_word = _fts_phrase_query(q)
            from_clause = """
                FROM chat_messages AS cm
                JOIN chat_logs AS cl ON cl.log_id = cm.log_id
                JOIN chat_messages_fts AS fts ON fts.rowid = cm.id
            """
        else:
            where_filter = "instr(lower(cm.msg), ?) > 0"
            query_word = q_lower
            from_clause = """
                FROM chat_messages AS cm
                JOIN chat_logs AS cl ON cl.log_id = cm.log_id
            """

        rows_sql = f"""
            WITH matches AS (
              SELECT
                cm.steamid64 AS steamid64,
                cm.steamid3 AS steamid3,
                cm.alias AS alias,
                cm.log_id AS log_id
              {from_clause}
              WHERE {where_filter}
                AND cm.steamid64 IS NOT NULL
                AND (? IS NULL OR cl.log_date_ts >= ?)
                AND (? IS NULL OR cl.log_date_ts <= ?)
                AND (? = '' OR instr(lower(cl.map), ?) > 0)
            ),
            agg AS (
              SELECT
                steamid64,
                steamid3,
                COALESCE(NULLIF(MAX(alias), ''), steamid3) AS name,
                COUNT(*) AS occurrences,
                COUNT(DISTINCT log_id) AS logs_count
              FROM matches
              GROUP BY steamid64, steamid3
            ),
            per_log AS (
              SELECT
                steamid64,
                log_id,
                COUNT(*) AS occurrences_in_log,
                ROW_NUMBER() OVER (
                  PARTITION BY steamid64
                  ORDER BY COUNT(*) DESC, log_id DESC
                ) AS rn
              FROM matches
              GROUP BY steamid64, log_id
            ),
            latest_log AS (
              SELECT steamid64, MAX(log_id) AS latest_log_id
              FROM matches
              GROUP BY steamid64
            )
            SELECT
              agg.steamid64,
              agg.steamid3,
              agg.name,
              agg.occurrences,
              agg.logs_count,
              COALESCE(per_log.log_id, latest_log.latest_log_id) AS top_log_id
            FROM agg
            LEFT JOIN per_log
              ON per_log.steamid64 = agg.steamid64
             AND per_log.rn = 1
            LEFT JOIN latest_log
              ON latest_log.steamid64 = agg.steamid64
            ORDER BY agg.occurrences DESC, agg.logs_count DESC, agg.name ASC
            LIMIT ?
        """
        logs_sql = f"""
            SELECT COUNT(DISTINCT cm.log_id)
            {from_clause}
            WHERE {where_filter}
              AND (? IS NULL OR cl.log_date_ts >= ?)
              AND (? IS NULL OR cl.log_date_ts <= ?)
              AND (? = '' OR instr(lower(cl.map), ?) > 0)
        """
        params: tuple[Any, ...] = (
            query_word,
            start_ts,
            start_ts,
            end_ts,
            end_ts,
            mq,
            mq,
            CHAT_SEARCH_LEADERBOARD_MAX_ROWS,
        )
        log_params: tuple[Any, ...] = (
            query_word,
            start_ts,
            start_ts,
            end_ts,
            end_ts,
            mq,
            mq,
        )
        out_rows = conn.execute(rows_sql, params).fetchall()
        logs_searched = int(conn.execute(logs_sql, log_params).fetchone()[0] or 0)
    finally:
        conn.close()

    rows: list[dict[str, Any]] = []
    for r in out_rows:
        sid64 = str(r[0] or "").strip()
        if not sid64:
            continue
        occurrences = int(r[3] or 0)
        logs_count = int(r[4] or 0)
        top_log_id = int(r[5] or 0)
        rows.append(
            {
                "steamid64": sid64,
                "steamid3": str(r[1] or ""),
                "name": str(r[2] or ""),
                "occurrences": occurrences,
                "logs_count": logs_count,
                "word_per_log": (occurrences / logs_count) if logs_count > 0 else 0.0,
                "top_log_id": top_log_id if top_log_id > 0 else None,
                "top_log_url": f"{LOGS_TF_URL_BASE}/{top_log_id}" if top_log_id > 0 else "",
                "profile_url": f"{LOGS_TF_URL_BASE}/profile/{sid64}",
            }
        )
    return rows, len(rows), logs_searched


def chat_search(
    word: str,
    steamid: str,
    logs_dir: str | Path,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    map_query: str = "",
) -> tuple[list[dict[str, Any]], int, str | None, frozenset[int]]:
    """
    Search chat for a player. Returns (results, total_count, searched_user_name, log_ids_used).

    log_ids_used: set of log IDs we had locally and considered (for cache invalidation).
    """
    logs_dir = Path(logs_dir)
    word = (word or "").strip()
    steamid = (steamid or "").strip()
    has_word = bool(word)
    word_lower = word.lower() if has_word else ""
    results: list[dict[str, Any]] = []
    log_ids_used: set[int] = set()

    steamid3 = steamid64_to_steamid3(steamid)
    log_ids = get_log_list_for_player(steamid)
    for log_id in log_ids:
        if len(results) >= CHAT_SEARCH_MAX_RESULTS_WITH_STEAMID:
            break
        path = logs_dir / f"{log_id}.json"
        if not path.exists():
            continue
        log_ids_used.add(log_id)
        try:
            data = path.read_text(encoding="utf-8", errors="replace")
            logtext = json.loads(data)
        except (OSError, ValueError):
            continue
        chat = logtext.get("chat")
        if not chat:
            continue
        info = logtext.get("info") or {}
        if not _log_in_date_range(info.get("date"), date_from, date_to):
            continue
        if not _map_matches_query(info.get("map"), map_query):
            continue
        players = logtext.get("players") or {}
        player_info = players.get(steamid3) if isinstance(players, dict) else None
        team_raw = player_info.get("team") if isinstance(player_info, dict) else None
        team = "Red" if team_raw == "Red" else ("Blue" if team_raw == "Blue" else None)
        for idx, msg in enumerate(chat):
            if msg.get("steamid") != steamid3:
                continue
            m = msg.get("msg") or ""
            if has_word and word_lower not in m.lower():
                continue
            alias = msg.get("name") or ""
            prev_entry = chat[idx - 1] if idx > 0 else None
            next_entry = chat[idx + 1] if idx + 1 < len(chat) else None

            def _ctx(entry: Any) -> dict[str, Any] | None:
                if not isinstance(entry, dict):
                    return None
                t = str(entry.get("msg") or "").strip()
                if not t:
                    return None
                if len(t) > CHAT_CONTEXT_PREVIEW_MAX_CHARS:
                    t = t[: CHAT_CONTEXT_PREVIEW_MAX_CHARS - 1] + "…"
                n = str(entry.get("name") or "").strip()
                sid3 = str(entry.get("steamid") or "").strip()
                ctx_team = None
                p = players.get(sid3) if isinstance(players, dict) and sid3 else None
                if isinstance(p, dict):
                    tr = p.get("team")
                    ctx_team = "Red" if tr == "Red" else ("Blue" if tr == "Blue" else None)
                return {"name": n, "msg": t, "team": ctx_team}

            results.append({
                "log_id": log_id,
                "alias": alias,
                "msg": m,
                "context_prev": _ctx(prev_entry),
                "context_next": _ctx(next_entry),
                "url": f"{LOGS_TF_URL_BASE}/{log_id}",
                "team": team,
            })
    searched_name = results[0]["alias"] if results else None
    if searched_name is not None:
        searched_name = searched_name.strip() or None
    return results, len(results), searched_name, frozenset(log_ids_used)


def _stats_search_files(
    steamid: str,
    gamemode: str,
    class_list: list[str],
    logs_dir: str | Path,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    map_query: str = "",
) -> tuple[list[dict[str, Any]], frozenset[int]]:
    """Stats by gamemode and classes (scan local JSON). Returns (rows, log_ids_used)."""
    logs_dir = Path(logs_dir)
    steamid3 = steamid64_to_steamid3(steamid)
    log_ids = get_log_list_for_player(steamid)
    class_set = set(c.strip().lower() for c in class_list if c)
    rows: list[dict[str, Any]] = []
    log_ids_used: set[int] = set()
    for log_id in log_ids:
        path = logs_dir / f"{log_id}.json"
        if not path.exists():
            continue
        log_ids_used.add(log_id)
        try:
            data = path.read_text(encoding="utf-8", errors="replace")
            logtext = json.loads(data)
        except (OSError, ValueError):
            continue
        names = logtext.get("names") or {}
        namesid = list(names.keys())
        if not _player_count_filter(len(namesid), gamemode):
            continue
        info = logtext.get("info") or {}
        if not _log_in_date_range(info.get("date"), date_from, date_to):
            continue
        if not _map_matches_query(info.get("map"), map_query):
            continue
        players = logtext.get("players") or {}
        stats = players.get(steamid3)
        if not stats:
            continue
        classstats = stats.get("class_stats")
        if not classstats:
            continue
        logclasslist = [c.get("type") for c in classstats if c.get("type")]
        for cls in class_set:
            if cls not in logclasslist:
                continue
            idx = logclasslist.index(cls)
            cs = classstats[idx]
            kills = int(cs.get("kills") or 0)
            assists = int(cs.get("assists") or 0)
            deaths = int(cs.get("deaths") or 0)
            if deaths == 0:
                kadr = kills + assists
                kdr = float(kills)
            else:
                kadr = round((kills + assists) / deaths, 2)
                kdr = round(kills / deaths, 2)
            dmg = int(cs.get("dmg") or 0)
            total_time = cs.get("total_time") or 1
            dpm = round((dmg / total_time) * 60, 2)
            hs = stats.get("headshots_hit") or 0
            bs = stats.get("backstabs") or 0
            map_name = info.get("map") or ""
            date_ts = info.get("date") or 0
            date_str = datetime.fromtimestamp(date_ts, tz=timezone.utc).strftime(
                "%I:%M:%S %p %Z %m/%d/%Y"
            )
            alias = names.get(steamid3) or ""
            team_raw = stats.get("team") if isinstance(stats, dict) else None
            team = "Red" if team_raw == "Red" else ("Blue" if team_raw == "Blue" else None)
            rows.append({
                "alias": alias,
                "team": team,
                "character": cls,
                "kills": kills,
                "assists": assists,
                "deaths": deaths,
                "kdr": kdr,
                "kadr": kadr,
                "dpm": dpm,
                "dmg": dmg,
                "headshots_hit": hs,
                "backstabs": bs,
                "map": map_name,
                "date": date_str,
                "url": f"{LOGS_TF_URL_BASE}/{log_id}",
            })
    return rows, frozenset(log_ids_used)


def stats_search(
    steamid: str,
    gamemode: str,
    class_list: list[str],
    logs_dir: str | Path,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    map_query: str = "",
) -> tuple[list[dict[str, Any]], frozenset[int]]:
    """Stats by gamemode and classes. Uses stats DB when populated; else scans JSON files."""
    sid64 = (steamid or "").strip()
    class_set = {c.strip().lower() for c in class_list if c.strip()}
    if not class_set:
        return _stats_search_files(
            steamid,
            gamemode,
            class_list,
            logs_dir,
            date_from=date_from,
            date_to=date_to,
            map_query=map_query,
        )
    if _stats_db_available_for_player(sid64):
        try:
            path = Path(STATS_DB_PATH)
            conn = _sqlite_connect_ro(path)
            try:
                gm_sql, gm_params = _gamemode_sql_stats(gamemode)
                start_ts, end_ts = _date_range_to_unix_bounds(date_from, date_to)
                map_q = (map_query or "").strip().lower()
                class_placeholders = ",".join("?" * len(class_set))
                class_tuple = tuple(sorted(class_set))
                sql = f"""
                    SELECT
                      lp.steamid64,
                      lp.steamid3,
                      lp.team,
                      lp.kills,
                      lp.assists,
                      lp.deaths,
                      lp.kdr,
                      lp.kadr,
                      lp.dapm,
                      lp.damage,
                      lp.headshots_hit,
                      lp.backstabs,
                      l.map,
                      l.date_ts,
                      l.log_id,
                      lp.primary_class
                    FROM log_players lp
                    JOIN logs l ON l.log_id = lp.log_id
                    WHERE lp.steamid64 = ?
                      AND EXISTS (
                        SELECT 1 FROM log_player_classes lpc
                        WHERE lpc.log_id = lp.log_id
                          AND lpc.steamid64 = lp.steamid64
                          AND lpc.class IN ({class_placeholders})
                      )
                """ + gm_sql
                params: list[Any] = [sid64, *class_tuple, *gm_params]
                if start_ts is not None:
                    sql += " AND l.date_ts >= ?"
                    params.append(start_ts)
                if end_ts is not None:
                    sql += " AND l.date_ts <= ?"
                    params.append(end_ts)
                if map_q:
                    sql += " AND instr(lower(l.map), ?) > 0"
                    params.append(map_q)
                sql += " ORDER BY l.date_ts DESC, l.log_id DESC"
                cur = conn.execute(sql, params)
                rows_out: list[dict[str, Any]] = []
                sid64_for_alias: list[str] = []
                for r in cur.fetchall():
                    (
                        p64,
                        _p3,
                        team_raw,
                        kills,
                        assists,
                        deaths,
                        kdr_v,
                        kadr_v,
                        dapm_v,
                        damage,
                        hs_hit,
                        bs,
                        map_name,
                        date_ts,
                        log_id,
                        primary_class,
                    ) = r
                    sid_s = str(p64 or "").strip()
                    if len(class_set) == 1:
                        character = next(iter(class_set))
                    else:
                        character = (str(primary_class).strip() if primary_class else "") or ""
                    ts = int(date_ts or 0)
                    date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
                        "%I:%M:%S %p %Z %m/%d/%Y"
                    )
                    team = "Red" if team_raw == "Red" else ("Blue" if team_raw == "Blue" else None)
                    sid64_for_alias.append(sid_s)
                    rows_out.append(
                        {
                            "alias": "",
                            "team": team,
                            "character": character,
                            "kills": int(kills or 0),
                            "assists": int(assists or 0),
                            "deaths": int(deaths or 0),
                            "kdr": _sql_float(kdr_v),
                            "kadr": _sql_float(kadr_v),
                            "dpm": round(_sql_float(dapm_v), 2),
                            "dmg": int(damage or 0),
                            "headshots_hit": int(hs_hit or 0),
                            "backstabs": int(bs or 0),
                            "map": str(map_name or ""),
                            "date": date_str,
                            "url": f"{LOGS_TF_URL_BASE}/{int(log_id)}",
                        }
                    )
            finally:
                conn.close()
            alias_map = _lookup_aliases_from_chat_db(sid64_for_alias)
            for row, sid_s in zip(rows_out, sid64_for_alias, strict=True):
                row["alias"] = alias_map.get(sid_s, "")
            cache_ids = stats_log_ids_for_player(STATS_DB_PATH, sid64)
            return rows_out, cache_ids
        except Exception as e:
            logger.warning("Stats DB search failed, using log files: %s", e)
    return _stats_search_files(
        steamid,
        gamemode,
        class_list,
        logs_dir,
        date_from=date_from,
        date_to=date_to,
        map_query=map_query,
    )


def _team_from_player_block(stats: Any) -> str | None:
    """Red / Blue from logs.tf player block, or None if unknown."""
    if not isinstance(stats, dict):
        return None
    tr = stats.get("team")
    if tr == "Red":
        return "Red"
    if tr == "Blue":
        return "Blue"
    return None


def _coplayers_search_files(
    steamid: str,
    logs_dir: str | Path,
    gamemode: str = "",
    map_query: str = "",
) -> tuple[list[dict[str, Any]], frozenset[int]]:
    """
    Frequent co-players for a player across local logs (scan JSON).
    Returns (rows sorted by total_games desc, log_ids_used) for cache invalidation.
    """
    logs_dir = Path(logs_dir)
    steamid3 = steamid64_to_steamid3(steamid)
    gm = (gamemode or "").strip()
    if gm not in ("hl", "7s", "6s", "ud"):
        gm = ""
    mq = map_query or ""
    log_ids_used: set[int] = set()
    agg: dict[str, dict[str, Any]] = {}
    log_ids = get_log_list_for_player(steamid)

    for log_id in log_ids:
        path = logs_dir / f"{log_id}.json"
        if not path.exists():
            continue
        try:
            data = path.read_text(encoding="utf-8", errors="replace")
            logtext = json.loads(data)
        except (OSError, ValueError):
            continue

        names = logtext.get("names") or {}
        if not isinstance(names, dict) or steamid3 not in names:
            continue

        names_keys = [k for k in names if isinstance(k, str)]
        n_players = len(names_keys)
        if gm and not _player_count_filter(n_players, gm):
            continue

        info = logtext.get("info") or {}
        if not _map_matches_query(info.get("map"), mq):
            continue

        players = logtext.get("players") or {}
        if not isinstance(players, dict):
            continue

        sub_stats = players.get(steamid3)
        subject_team = _team_from_player_block(sub_stats)
        if subject_team is None:
            continue

        log_ids_used.add(log_id)

        winner_team = _winner_team_from_log(logtext)

        try:
            log_ts = int(info.get("date") or 0)
        except (TypeError, ValueError):
            log_ts = 0

        for sid3 in names_keys:
            if sid3 == steamid3:
                continue
            opp_stats = players.get(sid3)
            opp_team = _team_from_player_block(opp_stats)
            if opp_team is None:
                continue

            bucket = agg.setdefault(
                sid3,
                {
                    "steamid3": sid3,
                    "name": "",
                    "_last_ts": -1,
                    "games_with": 0,
                    "wins_with": 0,
                    "losses_with": 0,
                    "games_against": 0,
                    "wins_against": 0,
                    "losses_against": 0,
                },
            )

            raw_name = names.get(sid3)
            nm = str(raw_name).strip() if raw_name is not None else ""
            if log_ts >= bucket["_last_ts"]:
                bucket["_last_ts"] = log_ts
                if nm:
                    bucket["name"] = nm

            same = opp_team == subject_team
            if same:
                bucket["games_with"] += 1
                if winner_team is not None:
                    if winner_team == subject_team:
                        bucket["wins_with"] += 1
                    else:
                        bucket["losses_with"] += 1
            else:
                bucket["games_against"] += 1
                if winner_team is not None:
                    if winner_team == subject_team:
                        bucket["wins_against"] += 1
                    else:
                        bucket["losses_against"] += 1

    rows: list[dict[str, Any]] = []
    for b in agg.values():
        total = int(b["games_with"]) + int(b["games_against"])
        if total < 2:
            continue
        sid64 = steamid3_to_steamid64(str(b["steamid3"]))
        rows.append(
            {
                "steamid3": b["steamid3"],
                "steamid64": sid64,
                "name": b["name"],
                "games_with": int(b["games_with"]),
                "wins_with": int(b["wins_with"]),
                "losses_with": int(b["losses_with"]),
                "games_against": int(b["games_against"]),
                "wins_against": int(b["wins_against"]),
                "losses_against": int(b["losses_against"]),
                "total_games": total,
            }
        )

    rows.sort(key=lambda r: -r["total_games"])
    return rows, frozenset(log_ids_used)


def coplayers_search(
    steamid: str,
    logs_dir: str | Path,
    gamemode: str = "",
    map_query: str = "",
) -> tuple[list[dict[str, Any]], frozenset[int]]:
    """Frequent co-players; uses stats DB when available, else scans JSON files."""
    sid64 = (steamid or "").strip()
    if not _stats_db_available_for_player(sid64):
        return _coplayers_search_files(steamid, logs_dir, gamemode=gamemode, map_query=map_query)
    try:
        path = Path(STATS_DB_PATH)
        conn = _sqlite_connect_ro(path)
        try:
            gm_sql, gm_params = _gamemode_sql_coplayers(gamemode)
            mq = (map_query or "").strip().lower()
            # Single aggregation in SQLite (was: fetch all logs + N chunked IN queries + Python
            # loops over every opponent row — O(logs × players/log) and could freeze the worker).
            inner = """
                SELECT
                  o.steamid64 AS steamid64,
                  MAX(o.steamid3) AS steamid3,
                  SUM(CASE WHEN o.team = s.team THEN 1 ELSE 0 END) AS games_with,
                  SUM(CASE WHEN o.team = s.team AND l.winner IS NOT NULL AND l.winner = s.team THEN 1 ELSE 0 END) AS wins_with,
                  SUM(CASE WHEN o.team = s.team AND l.winner IS NOT NULL AND l.winner <> s.team THEN 1 ELSE 0 END) AS losses_with,
                  SUM(CASE WHEN o.team <> s.team THEN 1 ELSE 0 END) AS games_against,
                  SUM(CASE WHEN o.team <> s.team AND l.winner IS NOT NULL AND l.winner = s.team THEN 1 ELSE 0 END) AS wins_against,
                  SUM(CASE WHEN o.team <> s.team AND l.winner IS NOT NULL AND l.winner <> s.team THEN 1 ELSE 0 END) AS losses_against
                FROM log_players AS s
                INNER JOIN logs AS l ON l.log_id = s.log_id
                INNER JOIN log_players AS o ON o.log_id = s.log_id AND o.steamid64 <> s.steamid64
                WHERE s.steamid64 = ?
                  AND s.team IN ('Red', 'Blue')
                  AND o.team IN ('Red', 'Blue')
            """ + gm_sql
            params: list[Any] = [sid64, *gm_params]
            if mq:
                inner += " AND instr(lower(l.map), ?) > 0"
                params.append(mq)
            inner += " GROUP BY o.steamid64"
            lim = int(_COPLAYERS_DB_RESULT_LIMIT)
            sql = f"""
                SELECT * FROM (
                {inner}
                ) AS agg
                WHERE agg.games_with + agg.games_against >= 2
                ORDER BY agg.games_with + agg.games_against DESC
                LIMIT {lim}
            """
            raw = conn.execute(sql, params).fetchall()
        finally:
            conn.close()

        rows: list[dict[str, Any]] = []
        for tup in raw:
            (
                opp64,
                opp3,
                gw,
                wiw,
                low,
                ga,
                wia,
                loa,
            ) = tup
            sid_opp = str(opp64 or "").strip()
            if not sid_opp:
                continue
            total = int(gw) + int(ga)
            rows.append(
                {
                    "steamid3": str(opp3 or "").strip(),
                    "steamid64": sid_opp,
                    "name": "",
                    "games_with": int(gw),
                    "wins_with": int(wiw),
                    "losses_with": int(low),
                    "games_against": int(ga),
                    "wins_against": int(wia),
                    "losses_against": int(loa),
                    "total_games": total,
                }
            )
        all_coplayer_sid64s = [r["steamid64"] for r in rows]
        alias_map = _lookup_aliases_from_chat_db(all_coplayer_sid64s)
        for row in rows:
            row["name"] = alias_map.get(row["steamid64"], "")
        cache_ids = stats_log_ids_for_player(STATS_DB_PATH, sid64)
        return rows, cache_ids
    except Exception as e:
        logger.warning("Co-players DB search failed, using log files: %s", e)
    return _coplayers_search_files(steamid, logs_dir, gamemode=gamemode, map_query=map_query)


def _player_stats_row_logmatch(
    steamid3: str,
    logtext: dict[str, Any],
    *,
    search_input: str,
    steamid64: str,
) -> dict[str, Any] | None:
    """One row of match stats for a player (logs.tf aggregate player block)."""
    players = logtext.get("players") or {}
    if not isinstance(players, dict):
        return None
    stats = players.get(steamid3)
    if not isinstance(stats, dict):
        return None
    names = logtext.get("names") or {}
    alias_raw = names.get(steamid3) if isinstance(names, dict) else ""
    alias = (str(alias_raw).strip() if alias_raw is not None else "") or ""
    team_raw = stats.get("team")
    team = "Red" if team_raw == "Red" else ("Blue" if team_raw == "Blue" else None)
    kills = int(stats.get("kills") or 0)
    deaths = int(stats.get("deaths") or 0)
    assists = int(stats.get("assists") or 0)
    if deaths == 0:
        kadr = float(kills + assists)
        kdr = float(kills)
    else:
        kadr = round((kills + assists) / deaths, 2)
        kdr = round(kills / deaths, 2)
    dmg = int(stats.get("dmg") or 0)
    dapm_raw = stats.get("dapm")
    if dapm_raw is not None:
        try:
            dpm = round(float(dapm_raw), 2)
        except (TypeError, ValueError):
            dpm = 0.0
    else:
        info = logtext.get("info") or {}
        length_sec = int(info.get("total_length") or logtext.get("length") or 0)
        if length_sec <= 0:
            length_sec = 1
        dpm = round((dmg / length_sec) * 60, 2)
    hs = int(stats.get("headshots_hit") or stats.get("headshots") or 0)
    bs = int(stats.get("backstabs") or 0)
    ubers = int(stats.get("ubers") or 0)
    drops = int(stats.get("drops") or 0)
    return {
        "alias": alias,
        "team": team,
        "search_input": search_input,
        "resolved_steamid64": steamid64,
        "class_playtime": _class_playtime_for_logmatch(stats),
        "kills": kills,
        "assists": assists,
        "deaths": deaths,
        "kdr": kdr,
        "kadr": kadr,
        "dpm": dpm,
        "dmg": dmg,
        "headshots_hit": hs,
        "backstabs": bs,
        "ubers": ubers,
        "drops": drops,
    }


def log_match(
    steamids: list[str],
    logs_dir: str | Path,
    *,
    search_inputs: list[str] | None = None,
    map_query: str = "",
) -> tuple[list[dict[str, Any]], int, frozenset[int], dict[str, Any] | None]:
    """Logs where all given players participated. Returns (results, total, matching_log_ids, head_to_head or None)."""
    logs_dir = Path(logs_dir)
    if not steamids:
        return [], 0, frozenset(), None
    labels: list[str] = (
        list(search_inputs)
        if search_inputs is not None and len(search_inputs) == len(steamids)
        else [str(s) for s in steamids]
    )
    steamid3s = [steamid64_to_steamid3(s) for s in steamids]
    steamid3_set = set(steamid3s)
    log_ids = get_log_list_for_player(steamids[0])
    results: list[dict[str, Any]] = []
    matching_log_ids: set[int] = set()
    for log_id in log_ids:
        path = logs_dir / f"{log_id}.json"
        if not path.exists():
            continue
        try:
            data = path.read_text(encoding="utf-8", errors="replace")
            logtext = json.loads(data)
        except (OSError, ValueError):
            continue
        names = logtext.get("names") or {}
        namesid = set(names.keys())
        if not steamid3_set.issubset(namesid):
            continue
        matching_log_ids.add(log_id)
        info = logtext.get("info") or {}
        title = info.get("title") or ""
        map_name = info.get("map") or ""
        if not _map_matches_query(map_name, map_query):
            continue
        date_ts = int(info.get("date") or 0)
        date_str = datetime.fromtimestamp(date_ts, tz=timezone.utc).strftime(
            "%m/%d/%Y %I:%M:%S %p %Z"
        )
        player_stats: list[dict[str, Any]] = []
        for i, sid3 in enumerate(steamid3s):
            row = _player_stats_row_logmatch(
                sid3,
                logtext,
                search_input=labels[i],
                steamid64=str(steamids[i]),
            )
            if row is not None:
                player_stats.append(row)
        results.append({
            "log_id": log_id,
            "title": title,
            "map": map_name,
            "date": date_str,
            "date_ts": date_ts,
            "url": f"{LOGS_TF_URL_BASE}/{log_id}",
            "player_stats": player_stats,
            "_winner_team": _winner_team_from_log(logtext),
        })
    head_to_head: dict[str, Any] | None = None
    if len(steamids) == 2:
        head_to_head = compute_head_to_head_summary(results, labels[0], labels[1])
    for r in results:
        r.pop("_winner_team", None)
    return results, len(results), frozenset(matching_log_ids), head_to_head


def compute_head_to_head_summary(
    results: list[dict[str, Any]],
    search_input_a: str,
    search_input_b: str,
) -> dict[str, Any]:
    """
    Head-to-head summary for exactly two players across log_match results.

    Partitions logs into 'opposing' (different teams) and 'same_team' buckets.
    Returns win/loss counts and average stat differentials (A minus B) for opposing logs.
    stat_diff > 0 means player A is ahead for that stat.
    """
    # Stat fields to diff (opposing logs only)
    DIFF_STATS = (
        "kills",
        "assists",
        "deaths",
        "dpm",
        "dmg",
        "kdr",
        "kadr",
        "ubers",
        "drops",
    )

    opposing_logs: list[dict[str, Any]] = []
    same_team_logs: list[dict[str, Any]] = []

    for r in results:
        stats = r.get("player_stats") or []
        a = next((s for s in stats if s.get("search_input") == search_input_a), None)
        b = next((s for s in stats if s.get("search_input") == search_input_b), None)
        if a is None or b is None:
            continue
        a_team = a.get("team")
        b_team = b.get("team")
        if a_team and b_team and a_team != b_team:
            opposing_logs.append({"a": a, "b": b, "winner": r.get("_winner_team")})
        elif a_team and b_team and a_team == b_team:
            same_team_logs.append(
                {"a": a, "b": b, "winner": r.get("_winner_team"), "team": a_team}
            )

    # --- Opposing ---
    opp_a_wins = opp_b_wins = opp_draws = 0
    opp_stat_totals: dict[str, float] = {s: 0.0 for s in DIFF_STATS}

    for entry in opposing_logs:
        a, b, winner = entry["a"], entry["b"], entry["winner"]
        a_team = a.get("team")
        b_team = b.get("team")
        if winner is None:
            opp_draws += 1
        elif winner == a_team:
            opp_a_wins += 1
        elif winner == b_team:
            opp_b_wins += 1
        else:
            opp_draws += 1
        for stat in DIFF_STATS:
            try:
                opp_stat_totals[stat] += float(a.get(stat) or 0) - float(b.get(stat) or 0)
            except (TypeError, ValueError):
                pass

    n_opp = len(opposing_logs)
    avg_diff = {
        stat: round(opp_stat_totals[stat] / n_opp, 2) if n_opp else 0.0
        for stat in DIFF_STATS
    }

    # --- Same team ---
    same_wins = same_losses = same_draws = 0
    for entry in same_team_logs:
        winner = entry["winner"]
        team = entry["team"]
        if winner is None:
            same_draws += 1
        elif winner == team:
            same_wins += 1
        elif winner in ("Red", "Blue") and winner != team:
            same_losses += 1
        else:
            same_draws += 1

    return {
        "player_a_label": search_input_a,
        "player_b_label": search_input_b,
        "opposing": {
            "logs_count": n_opp,
            "player_a_wins": opp_a_wins,
            "player_b_wins": opp_b_wins,
            "draws": opp_draws,
            "avg_stat_diff": avg_diff,  # positive = A ahead
        },
        "same_team": {
            "logs_count": len(same_team_logs),
            "wins": same_wins,
            "losses": same_losses,
            "draws": same_draws,
        },
    }


def _profile_filter_sql(
    gamemode: str,
    date_from: date | None,
    date_to: date | None,
    map_query: str,
) -> tuple[str, list[Any]]:
    gm_sql, gm_params = _gamemode_sql_stats(gamemode)
    start_ts, end_ts = _date_range_to_unix_bounds(date_from, date_to)
    map_q = (map_query or "").strip().lower()
    parts: list[str] = [gm_sql]
    params: list[Any] = list(gm_params)
    if start_ts is not None:
        parts.append(" AND l.date_ts >= ?")
        params.append(start_ts)
    if end_ts is not None:
        parts.append(" AND l.date_ts <= ?")
        params.append(end_ts)
    if map_q:
        parts.append(" AND instr(lower(l.map), ?) > 0")
        params.append(map_q)
    return "".join(parts), params


def _round2(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return round(float(v), 2)
    except (TypeError, ValueError):
        return None


# logs.tf ``class_stats[].type`` values for playable classes (icons in UI).
_PROFILE_MAIN_CLASSES: frozenset[str] = frozenset({
    "scout",
    "soldier",
    "pyro",
    "demoman",
    "heavyweapons",
    "engineer",
    "medic",
    "sniper",
    "spy",
})


def _class_label_norm(raw: Any) -> str:
    return str(raw or "").strip().lower()


def _split_profile_class_rows(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Primary table = 9 playable classes; ``classes_other`` = spectator, mods, junk not in the icon set."""
    main: list[dict[str, Any]] = []
    other: list[dict[str, Any]] = []
    for r in rows:
        cn = _class_label_norm(r.get("class"))
        if not cn or cn in ("undefined", "none"):
            continue
        if cn in _PROFILE_MAIN_CLASSES:
            main.append(r)
        else:
            other.append(r)
    return main, other


def player_profile(
    steamid64: str,
    *,
    gamemode: str = "",
    date_from: date | None = None,
    date_to: date | None = None,
    map_query: str = "",
) -> tuple[dict[str, Any], frozenset[int]]:
    """
    Full aggregated player profile from stats DB.
    Returns (profile_dict, log_ids_used for cache invalidation).
    Raises RuntimeError if stats DB is unavailable for this player.
    """
    sid = (steamid64 or "").strip()
    if not sid:
        raise RuntimeError("Stats DB not available for this player.")
    if not _stats_db_available_for_player(sid):
        raise RuntimeError("Stats DB not available for this player.")

    path = Path(STATS_DB_PATH)
    filter_sql, filter_params = _profile_filter_sql(gamemode, date_from, date_to, map_query)
    gm_used = (gamemode or "").strip()
    if gm_used not in ("", "hl", "7s", "6s", "ud"):
        gm_used = ""

    conn = _sqlite_connect_ro(path)
    healed_to_raw: list[tuple[str, int, int]] = []
    healed_by_raw: list[tuple[str, int, int]] = []
    try:
        # --- Overview ---
        overview_sql = f"""
            SELECT
              COUNT(*) AS logs_count,
              SUM(lp.kills) AS total_kills,
              SUM(lp.assists) AS total_assists,
              SUM(lp.deaths) AS total_deaths,
              SUM(lp.damage) AS total_damage,
              SUM(lp.damage_taken) AS total_damage_taken,
              SUM(lp.ubers) AS total_ubers,
              SUM(lp.drops) AS total_drops,
              SUM(lp.healing_taken) AS total_healing_taken,
              SUM(lp.captures) AS total_captures,
              SUM(lp.dominated) AS total_dominated,
              SUM(lp.revenges) AS total_revenges,
              SUM(lp.suicides) AS total_suicides,
              AVG(CASE WHEN lp.dapm IS NOT NULL THEN lp.dapm END) AS avg_dpm,
              AVG(CASE WHEN lp.kdr IS NOT NULL THEN lp.kdr END) AS avg_kdr,
              AVG(CASE WHEN lp.kadr IS NOT NULL THEN lp.kadr END) AS avg_kadr,
              AVG(lp.kills) AS avg_kills,
              AVG(lp.assists) AS avg_assists,
              AVG(lp.deaths) AS avg_deaths,
              MAX(lp.longest_killstreak) AS best_killstreak,
              SUM(CASE WHEN l.winner IS NOT NULL AND l.winner = lp.team THEN 1 ELSE 0 END) AS wins,
              SUM(CASE WHEN l.winner IS NOT NULL AND l.winner != lp.team THEN 1 ELSE 0 END) AS losses,
              MIN(l.date_ts) AS first_log_ts,
              MAX(l.date_ts) AS last_log_ts
            FROM log_players lp
            JOIN logs l ON l.log_id = lp.log_id
            WHERE lp.steamid64 = ?
              {filter_sql}
        """
        row = conn.execute(overview_sql, (sid, *filter_params)).fetchone()
        logs_count = int(row[0] or 0) if row else 0

        def _i(idx: int) -> int:
            if not row:
                return 0
            v = row[idx]
            return int(v) if v is not None else 0

        wins = _i(20)
        losses = _i(21)
        wl = wins + losses
        win_rate = round(wins / wl, 4) if wl > 0 else None
        draws = logs_count - wins - losses if logs_count else 0

        overview: dict[str, Any] = {
            "total_kills": _i(1),
            "total_assists": _i(2),
            "total_deaths": _i(3),
            "total_damage": _i(4),
            "total_damage_taken": _i(5),
            "total_ubers": _i(6),
            "total_drops": _i(7),
            "total_healing_taken": _i(8),
            "total_captures": _i(9),
            "total_dominated": _i(10),
            "total_revenges": _i(11),
            "total_suicides": _i(12),
            "avg_dpm": _round2(row[13]) if row and logs_count else None,
            "avg_kdr": _round2(row[14]) if row and logs_count else None,
            "avg_kadr": _round2(row[15]) if row and logs_count else None,
            "avg_kills": _round2(row[16]) if row and logs_count else None,
            "avg_assists": _round2(row[17]) if row and logs_count else None,
            "avg_deaths": _round2(row[18]) if row and logs_count else None,
            "win_rate": win_rate,
            "wins": wins,
            "losses": losses,
            "draws": draws,
            "best_killstreak": int(row[19]) if row and logs_count and row[19] is not None else None,
            "most_played_class": None,
            "first_log_ts": int(row[22]) if row and row[22] is not None else None,
            "last_log_ts": int(row[23]) if row and row[23] is not None else None,
            "logs_count": logs_count,
        }

        mpc_sql = f"""
            SELECT lp.primary_class, COUNT(*) AS n
            FROM log_players lp
            JOIN logs l ON l.log_id = lp.log_id
            WHERE lp.steamid64 = ?
              AND lp.primary_class IS NOT NULL
              AND trim(lp.primary_class) != ''
              AND lower(trim(lp.primary_class)) NOT IN ('undefined', 'none')
              {filter_sql}
            GROUP BY lp.primary_class
            ORDER BY n DESC
            LIMIT 1
        """
        mpc_row = conn.execute(mpc_sql, (sid, *filter_params)).fetchone()
        if mpc_row and mpc_row[0]:
            overview["most_played_class"] = _class_label_norm(mpc_row[0])

        # --- Classes ---
        classes_sql = f"""
            SELECT
              lpc.class,
              COUNT(DISTINCT lpc.log_id) AS logs_count,
              SUM(lpc.playtime) AS total_playtime_secs,
              SUM(lpc.kills) AS total_kills,
              SUM(lpc.assists) AS total_assists,
              SUM(lpc.deaths) AS total_deaths,
              SUM(lpc.damage) AS total_damage
            FROM log_player_classes lpc
            JOIN logs l ON l.log_id = lpc.log_id
            WHERE lpc.steamid64 = ?
              AND trim(lpc.class) != ''
              AND lower(trim(lpc.class)) NOT IN ('undefined', 'none')
              {filter_sql}
            GROUP BY lpc.class
            HAVING SUM(lpc.playtime) > 0
            ORDER BY total_playtime_secs DESC
        """
        classes_out: list[dict[str, Any]] = []
        for cr in conn.execute(classes_sql, (sid, *filter_params)).fetchall():
            lc = int(cr[1] or 0)
            pt = int(cr[2] or 0)
            tk = int(cr[3] or 0)
            ta = int(cr[4] or 0)
            tdeaths = int(cr[5] or 0)
            dmg = int(cr[6] or 0)
            avg_dpm = round((dmg / float(pt)) * 60.0, 2) if pt > 0 else None
            avg_kdr = round(tk / tdeaths, 2) if tdeaths > 0 else None
            classes_out.append({
                "class": _class_label_norm(cr[0]),
                "logs_count": lc,
                "total_playtime_secs": pt,
                "total_kills": tk,
                "total_assists": ta,
                "total_deaths": tdeaths,
                "total_damage": dmg,
                "avg_kills": round(tk / lc, 2) if lc else None,
                "avg_deaths": round(tdeaths / lc, 2) if lc else None,
                "avg_dpm": avg_dpm,
                "avg_kdr": avg_kdr,
            })

        classes_main, classes_other = _split_profile_class_rows(classes_out)

        # --- Weapons ---
        weapons_sql = f"""
            SELECT
              lpw.weapon,
              SUM(lpw.kills) AS total_kills,
              SUM(lpw.damage) AS total_damage,
              SUM(lpw.shots) AS total_shots,
              SUM(lpw.hits) AS total_hits,
              COUNT(DISTINCT lpw.log_id) AS logs_count
            FROM log_player_weapons lpw
            JOIN logs l ON l.log_id = lpw.log_id
            WHERE lpw.steamid64 = ?
              {filter_sql}
            GROUP BY lpw.weapon
            HAVING SUM(lpw.kills) > 0 OR SUM(lpw.damage) > 0
            ORDER BY total_kills DESC
            LIMIT 30
        """
        weapons_out: list[dict[str, Any]] = []
        for wr in conn.execute(weapons_sql, (sid, *filter_params)).fetchall():
            tk = int(wr[1] or 0)
            tdmg = int(wr[2] or 0)
            ts = int(wr[3] or 0)
            th = int(wr[4] or 0)
            lcw = int(wr[5] or 0)
            acc = round(th / ts, 4) if ts > 0 else None
            adph = round(tdmg / th, 2) if th > 0 else None
            weapons_out.append({
                "weapon": str(wr[0]),
                "total_kills": tk,
                "total_damage": tdmg,
                "total_shots": ts,
                "total_hits": th,
                "accuracy": acc,
                "avg_damage_per_shot": adph,
                "logs_count": lcw,
            })

        # --- Class kills ---
        ck_sql = f"""
            SELECT
              lpck.victim_class,
              SUM(lpck.kills) AS total_kills
            FROM log_player_classkills lpck
            JOIN logs l ON l.log_id = lpck.log_id
            WHERE lpck.steamid64 = ?
              {filter_sql}
            GROUP BY lpck.victim_class
            ORDER BY total_kills DESC
        """
        class_kills_out = [
            {"victim_class": _class_label_norm(x[0]), "total_kills": int(x[1] or 0)}
            for x in conn.execute(ck_sql, (sid, *filter_params)).fetchall()
            if _class_label_norm(x[0]) not in ("", "undefined", "none")
        ]

        # --- Rounds ---
        rounds_a_sql = f"""
            SELECT
              COUNT(*) AS total_rounds,
              SUM(CASE WHEN r.duration_secs IS NOT NULL THEN 1 ELSE 0 END) AS rounds_with_data,
              AVG(r.duration_secs) AS avg_duration,
              SUM(CASE WHEN r.first_blood_steamid64 = ? THEN 1 ELSE 0 END) AS first_bloods,
              SUM(CASE WHEN r.winner = 'Red' THEN 1 ELSE 0 END) AS red_round_wins,
              SUM(CASE WHEN r.winner = 'Blue' THEN 1 ELSE 0 END) AS blue_round_wins
            FROM log_rounds r
            JOIN log_players lp ON lp.log_id = r.log_id AND lp.steamid64 = ?
            JOIN logs l ON l.log_id = r.log_id
            WHERE 1=1
              {filter_sql}
        """
        ra = conn.execute(rounds_a_sql, (sid, sid, *filter_params)).fetchone()
        rounds_b_sql = f"""
            SELECT COUNT(*) AS round_wins_on_team
            FROM log_rounds r
            JOIN log_players lp ON lp.log_id = r.log_id AND lp.steamid64 = ?
            JOIN logs l ON l.log_id = r.log_id
            WHERE r.winner IS NOT NULL
              AND r.winner = lp.team
              {filter_sql}
        """
        rb = conn.execute(rounds_b_sql, (sid, *filter_params)).fetchone()
        total_rounds = int(ra[0] or 0) if ra else 0
        rounds_with_data = int(ra[1] or 0) if ra else 0
        avg_dur = float(ra[2]) if ra and ra[2] is not None else None
        first_bloods = int(ra[3] or 0) if ra else 0
        red_rw = int(ra[4] or 0) if ra else 0
        blue_rw = int(ra[5] or 0) if ra else 0
        round_wins_on_team = int(rb[0] or 0) if rb else 0
        first_blood_rate = round(first_bloods / total_rounds, 4) if total_rounds > 0 else None
        round_win_rate_on_team = round(round_wins_on_team / total_rounds, 4) if total_rounds > 0 else None
        rounds_out: dict[str, Any] = {
            "total_rounds": total_rounds,
            "rounds_with_data": rounds_with_data,
            "first_bloods": first_bloods,
            "first_blood_rate": first_blood_rate,
            "avg_round_duration_secs": _round2(avg_dur) if avg_dur is not None else None,
            "red_round_wins": red_rw,
            "blue_round_wins": blue_rw,
            "round_wins_on_team": round_wins_on_team,
            "round_win_rate_on_team": round_win_rate_on_team,
        }

        # --- Healspread (names resolved after connection closes) ---
        ht_sql = f"""
            SELECT
              lph.patient_steamid64,
              SUM(lph.healing) AS total_healing,
              COUNT(DISTINCT lph.log_id) AS logs_count
            FROM log_player_healspread lph
            JOIN logs l ON l.log_id = lph.log_id
            WHERE lph.healer_steamid64 = ?
              {filter_sql}
            GROUP BY lph.patient_steamid64
            ORDER BY total_healing DESC
            LIMIT 10
        """
        hb_sql = f"""
            SELECT
              lph.healer_steamid64,
              SUM(lph.healing) AS total_healing,
              COUNT(DISTINCT lph.log_id) AS logs_count
            FROM log_player_healspread lph
            JOIN logs l ON l.log_id = lph.log_id
            WHERE lph.patient_steamid64 = ?
              {filter_sql}
            GROUP BY lph.healer_steamid64
            ORDER BY total_healing DESC
            LIMIT 10
        """
        healed_to_raw = [
            (str(a[0]), int(a[1] or 0), int(a[2] or 0))
            for a in conn.execute(ht_sql, (sid, *filter_params)).fetchall()
        ]
        healed_by_raw = [
            (str(a[0]), int(a[1] or 0), int(a[2] or 0))
            for a in conn.execute(hb_sql, (sid, *filter_params)).fetchall()
        ]
    finally:
        conn.close()

    partner_ids: list[str] = []
    for p, _, _ in healed_to_raw:
        partner_ids.append(p)
    for p, _, _ in healed_by_raw:
        partner_ids.append(p)
    name_map = _lookup_aliases_from_chat_db([sid] + partner_ids)
    display_name = (name_map.get(sid) or "").strip()

    healed_to = [
        {
            "steamid64": p,
            "name": (name_map.get(p) or "").strip(),
            "total_healing": h,
            "logs_count": lc,
        }
        for p, h, lc in healed_to_raw
    ]
    healed_by = [
        {
            "steamid64": p,
            "name": (name_map.get(p) or "").strip(),
            "total_healing": h,
            "logs_count": lc,
        }
        for p, h, lc in healed_by_raw
    ]

    log_ids = stats_log_ids_for_player(STATS_DB_PATH, sid)
    profile: dict[str, Any] = {
        "steamid64": sid,
        "display_name": display_name,
        "logs_count": logs_count,
        "filters_applied": {
            "gamemode": gm_used,
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
            "map_query": (map_query or "").strip(),
        },
        "overview": overview,
        "classes": classes_main,
        "classes_other": classes_other,
        "weapons": weapons_out,
        "class_kills": class_kills_out,
        "rounds": rounds_out,
        "healspread": {"healed_to": healed_to, "healed_by": healed_by},
    }
    return profile, log_ids


def log_match_matching_log_ids(steamids: list[str], logs_dir: str | Path) -> frozenset[int]:
    """Return the set of log IDs that contain all given players (for cache invalidation without building full result)."""
    logs_dir = Path(logs_dir)
    if not steamids:
        return frozenset()
    steamid3s = [steamid64_to_steamid3(s) for s in steamids]
    steamid3_set = set(steamid3s)
    log_ids = get_log_list_for_player(steamids[0])
    out: set[int] = set()
    for log_id in log_ids:
        path = logs_dir / f"{log_id}.json"
        if not path.exists():
            continue
        try:
            data = path.read_text(encoding="utf-8", errors="replace")
            logtext = json.loads(data)
        except (OSError, ValueError):
            continue
        names = logtext.get("names") or {}
        if steamid3_set.issubset(set(names.keys())):
            out.add(log_id)
    return frozenset(out)


# Player-by-name search (chat DB): substring match on alias, bounded result size.
PLAYER_NAME_SEARCH_MAX_ROWS = 200
# Trigram FTS5 matches substrings reliably for needles of this length or more.
_PLAYER_NAME_FTS_MIN_LEN = 3


class PlayerNameIndexNotReadyError(Exception):
    """chat_messages has rows but alias FTS was not rebuilt / chat_app_meta.alias_fts_ready is unset."""


def _fts5_trigram_phrase(needle_lower: str) -> str:
    """Double-quoted FTS5 phrase for trigram tokenizer (substring, ASCII case-fold)."""
    return '"' + needle_lower.replace('"', '""') + '"'


def _player_name_use_alias_fts(conn: sqlite3.Connection) -> bool:
    """
    Use trigram FTS only when downloader/backfill marked the index complete in chat_app_meta.

    Avoids MATCH when the FTS table is empty or only partially filled (triggers without rebuild).
    """
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='chat_messages_alias_fts'"
    ).fetchone()
    if row is None:
        return False
    if conn.execute("SELECT 1 FROM chat_messages LIMIT 1").fetchone() is None:
        return True
    try:
        ready = conn.execute(
            "SELECT value FROM chat_app_meta WHERE key = ? LIMIT 1",
            (CHAT_ALIAS_FTS_READY_META_KEY,),
        ).fetchone()
    except sqlite3.OperationalError:
        return False
    return ready is not None and ready[0] == "1"


def _player_name_require_index_ready(conn: sqlite3.Connection) -> None:
    """
    Refuse to run a full-table scan: if we have chat rows, the alias index must be marked ready.

    Empty database: no-op.
    """
    if conn.execute("SELECT 1 FROM chat_messages LIMIT 1").fetchone() is None:
        return
    try:
        ready = conn.execute(
            "SELECT value FROM chat_app_meta WHERE key = ? LIMIT 1",
            (CHAT_ALIAS_FTS_READY_META_KEY,),
        ).fetchone()
    except sqlite3.OperationalError:
        ready = None
    if ready is None or ready[0] != "1":
        raise PlayerNameIndexNotReadyError(
            "Player name index is still building. Try again after the downloader finishes the alias FTS step, "
            "or run: python -m app.rebuild_alias_fts (stop the downloader first for fastest rebuild)."
        )


def _player_name_fetchall_retry(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple[Any, ...],
    *,
    attempts: int = 12,
    sleep_sec: float = 0.35,
) -> list[Any]:
    last: sqlite3.OperationalError | None = None
    for _ in range(attempts):
        try:
            return conn.execute(sql, params).fetchall()
        except sqlite3.OperationalError as e:
            last = e
            err = str(e).lower()
            if "locked" not in err and "busy" not in err:
                raise
            time.sleep(sleep_sec)
    assert last is not None
    raise last


def player_name_search_sqlite(
    name_substr: str,
    db_path: str | Path,
    *,
    limit: int = PLAYER_NAME_SEARCH_MAX_ROWS,
) -> list[dict[str, Any]]:
    """
    Find players whose chat alias contains name_substr (case-insensitive).

    One row per Steam account (steamid3), sorted by number of distinct logs with a
    matching alias, then by total matching chat lines.

    Uses FTS5 trigram on alias when the index is ready and needle length >= 3; otherwise
    uses instr() on alias (only after the index is marked ready — never full-scans
    an unindexed multi-million-row table). Needles shorter than 3 characters return no rows.
    """
    path = Path(db_path)
    if not path.is_file():
        return []
    needle = (name_substr or "").strip().lower()
    if not needle:
        return []
    if len(needle) < _PLAYER_NAME_FTS_MIN_LEN:
        return []
    lim = max(1, min(int(limit), PLAYER_NAME_SEARCH_MAX_ROWS))

    conn = sqlite3.connect(
        path.resolve().as_uri() + "?mode=ro",
        uri=True,
        timeout=60.0,
        check_same_thread=False,
    )
    try:
        conn.execute("PRAGMA busy_timeout=60000")
        _player_name_require_index_ready(conn)
        use_fts = len(needle) >= _PLAYER_NAME_FTS_MIN_LEN and _player_name_use_alias_fts(conn)
        if use_fts:
            match_arg = _fts5_trigram_phrase(needle)
            # FTS5 MATCH must use the virtual table name; some SQLite builds reject a table
            # alias here ("no such column: af").
            filter_clause = (
                "INNER JOIN chat_messages_alias_fts "
                "ON chat_messages_alias_fts.rowid = cm.id "
                "WHERE chat_messages_alias_fts MATCH ? AND TRIM(cm.steamid3) != ''"
            )
            params: tuple[Any, ...] = (match_arg, lim)
        else:
            filter_clause = (
                "WHERE instr(lower(cm.alias), ?) > 0 AND TRIM(cm.steamid3) != ''"
            )
            params = (needle, lim)

        sql = (
            "WITH filtered AS ("
            "  SELECT cm.steamid3, NULLIF(TRIM(cm.steamid64), '') AS steamid64,"
            "    cm.alias, cm.log_id, cm.message_idx, cl.log_date_ts"
            "  FROM chat_messages cm"
            "  INNER JOIN chat_logs cl ON cl.log_id = cm.log_id "
            + filter_clause +
            "), ranked AS ("
            "  SELECT steamid3, steamid64, alias, log_id, message_idx,"
            "    ROW_NUMBER() OVER ("
            "      PARTITION BY steamid3"
            "      ORDER BY log_date_ts DESC, log_id DESC, message_idx DESC"
            "    ) AS rn"
            "  FROM filtered"
            ") "
            "SELECT steamid3, MAX(steamid64) AS steamid64_raw,"
            "  MAX(CASE WHEN rn = 1 THEN alias END) AS display_name,"
            "  COUNT(DISTINCT log_id) AS logs_count, COUNT(*) AS messages_count "
            "FROM ranked GROUP BY steamid3 "
            "ORDER BY logs_count DESC, messages_count DESC, steamid3 ASC LIMIT ?"
        )
        rows = _player_name_fetchall_retry(conn, sql, params)
    finally:
        conn.close()

    out: list[dict[str, Any]] = []
    for steamid3, steamid64_raw, display_name, logs_count, messages_count in rows:
        sid64 = (str(steamid64_raw).strip() if steamid64_raw else "")
        if len(sid64) != 17 or not sid64.isdigit():
            sid64 = steamid3_to_steamid64(steamid3) or ""
        if not sid64:
            continue
        disp = (display_name or "").strip() or sid64
        out.append({
            "steamid64": sid64,
            "display_name": disp,
            "logs_count": int(logs_count or 0),
            "messages_count": int(messages_count or 0),
        })
    return out
