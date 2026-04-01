"""Search logic: chat search, stats search, log match. Pure Python, no HTTP."""
import json
import re
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from app.logs_tf import get_log_list_for_player, steamid3_to_steamid64, steamid64_to_steamid3

LOGS_TF_URL_BASE = "https://logs.tf"

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
    """Stats by gamemode and classes. Returns (rows, log_ids_used) for table rendering and cache invalidation."""
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


def _team_score_from_teams_block(teams: Any, team_key: str) -> int | None:
    """Integer score for Red or Blue from logs.tf ``teams`` object."""
    if not isinstance(teams, dict):
        return None
    block = teams.get(team_key)
    if not isinstance(block, dict):
        return None
    raw = block.get("score")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _winner_team_from_info_field(w: Any) -> str | None:
    """Normalize ``info.winner`` to Red / Blue when unambiguous."""
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


def _winner_team_from_log(logtext: dict[str, Any]) -> str | None:
    """
    Winning team as Red or Blue.

    Prefer ``info.winner`` when it maps cleanly; otherwise infer from
    ``teams.Red.score`` vs ``teams.Blue.score`` (logs.tf often leaves winner null).
    Ties or missing scores => unknown (None).
    """
    info = logtext.get("info")
    if isinstance(info, dict):
        parsed = _winner_team_from_info_field(info.get("winner"))
        if parsed is not None:
            return parsed
    teams = logtext.get("teams")
    rs = _team_score_from_teams_block(teams, "Red")
    bs = _team_score_from_teams_block(teams, "Blue")
    if rs is None or bs is None:
        return None
    if rs > bs:
        return "Red"
    if bs > rs:
        return "Blue"
    return None


def coplayers_search(
    steamid: str,
    logs_dir: str | Path,
    gamemode: str = "",
    map_query: str = "",
) -> tuple[list[dict[str, Any]], frozenset[int]]:
    """
    Frequent co-players for a player across local logs.
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
) -> tuple[list[dict[str, Any]], int, frozenset[int]]:
    """Logs where all given players participated. Returns (results, total, matching_log_ids) for cache invalidation."""
    logs_dir = Path(logs_dir)
    if not steamids:
        return [], 0, frozenset()
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
        })
    return results, len(results), frozenset(matching_log_ids)


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
