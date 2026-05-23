"""Disk cache for default player profile and co-players payloads (shared SQLite file)."""
from __future__ import annotations

import json
import logging
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from app.config import PROFILE_CACHE_DB_PATH, PROFILE_CACHE_MAX_AGE_SEC

logger = logging.getLogger(__name__)

_STEAMID64_RE = re.compile(r"^\d{17}$")
# Bound worst-case blob size (DoS / accidental huge payloads).
_MAX_PAYLOAD_BYTES = 52_428_800  # 50 MiB
_write_lock = threading.Lock()


def connect_profile_cache_db(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30.0)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_profile_cache_db(conn: sqlite3.Connection) -> None:
    """Create profile and co-players disk cache tables if missing."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS profile_cache (
          steamid64 TEXT PRIMARY KEY,
          payload_json TEXT NOT NULL,
          token_count INTEGER NOT NULL,
          token_max_log_id INTEGER NOT NULL,
          token_sum_imported_at INTEGER NOT NULL,
          built_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS coplayers_cache (
          steamid64 TEXT NOT NULL,
          gamemode TEXT NOT NULL DEFAULT '',
          map_query TEXT NOT NULL DEFAULT '',
          payload_json TEXT NOT NULL,
          token_count INTEGER NOT NULL,
          token_max_log_id INTEGER NOT NULL,
          token_sum_imported_at INTEGER NOT NULL,
          built_at INTEGER NOT NULL,
          PRIMARY KEY (steamid64, gamemode, map_query)
        );
        """
    )
    conn.commit()


def is_default_profile_scope(
    *,
    gamemode: str,
    date_from: Any,
    date_to: Any,
    map_query: str,
) -> bool:
    """True when the request matches the disk-cached default profile key."""
    return (
        not (gamemode or "").strip()
        and date_from is None
        and date_to is None
        and not (map_query or "").strip()
    )


def default_profile_cache_key(steamid64: str) -> tuple[str, str, str, str, str]:
    return ((steamid64 or "").strip(), "", "", "", "")


def is_default_coplayers_scope(*, gamemode: str, map_query: str) -> bool:
    """True when the request matches the disk-cached default co-players key."""
    return not (gamemode or "").strip() and not (map_query or "").strip()


def default_coplayers_cache_key(steamid64: str) -> tuple[str, str, str]:
    return ((steamid64 or "").strip(), "", "")


def _read_cached_payload(
    conn: sqlite3.Connection,
    table: str,
    where_sql: str,
    where_params: tuple[Any, ...],
    current_token: tuple[int, int, int],
    log_label: str,
) -> dict[str, Any] | None:
    cur_cnt, cur_max, cur_sum = (int(current_token[0]), int(current_token[1]), int(current_token[2]))
    try:
        row = conn.execute(
            f"""
            SELECT payload_json, token_count, token_max_log_id, token_sum_imported_at, built_at
            FROM {table}
            WHERE {where_sql}
            """,
            where_params,
        ).fetchone()
    except sqlite3.Error as e:
        logger.warning("%s read failed: %s", log_label, e)
        return None
    if not row:
        return None
    payload_json, tok_cnt, tok_max, tok_sum, built_at = row
    try:
        age = time.time() - int(built_at)
    except (TypeError, ValueError):
        return None
    if age > PROFILE_CACHE_MAX_AGE_SEC:
        return None
    if (int(tok_cnt), int(tok_max), int(tok_sum)) != (cur_cnt, cur_max, cur_sum):
        return None
    if not payload_json:
        return None
    try:
        if len(payload_json) > _MAX_PAYLOAD_BYTES:
            logger.warning("%s row too large (%s bytes)", log_label, len(payload_json))
            return None
        data = json.loads(payload_json)
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning("%s JSON invalid: %s", log_label, e)
        return None
    if not isinstance(data, dict):
        return None
    return data


def _write_cached_payload(
    conn: sqlite3.Connection,
    table: str,
    columns: tuple[str, ...],
    values: tuple[Any, ...],
    log_label: str,
) -> bool:
    placeholders = ",".join("?" * len(columns))
    col_list = ", ".join(columns)
    try:
        with _write_lock:
            conn.execute(
                f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})",
                values,
            )
            conn.commit()
        return True
    except sqlite3.Error as e:
        logger.warning("%s write failed: %s", log_label, e)
        return False


def get_default_profile_cache(
    conn: sqlite3.Connection,
    steamid64: str,
    current_token: tuple[int, int, int],
) -> dict[str, Any] | None:
    """
    Return cached profile dict when present, within max age, and token matches ``current_token``.

    ``current_token`` must be ``(log_count, max_log_id, sum_imported_at)`` from
    ``stats_player_stats_cache_token_parts`` (not a frozenset — order is semantic).
    """
    sid = (steamid64 or "").strip()
    if not _STEAMID64_RE.fullmatch(sid):
        return None
    return _read_cached_payload(
        conn,
        "profile_cache",
        "steamid64 = ?",
        (sid,),
        current_token,
        f"profile_cache/{sid}",
    )


def set_default_profile_cache(
    conn: sqlite3.Connection,
    steamid64: str,
    payload: dict[str, Any],
    token: tuple[int, int, int],
) -> bool:
    """Upsert default profile payload. Returns False if validation or size checks fail."""
    sid = (steamid64 or "").strip()
    if not _STEAMID64_RE.fullmatch(sid) or not isinstance(payload, dict):
        return False
    try:
        payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError) as e:
        logger.warning("profile_cache serialize failed for %s: %s", sid, e)
        return False
    if len(payload_json) > _MAX_PAYLOAD_BYTES:
        logger.warning(
            "profile_cache skip store for %s: payload %s bytes exceeds cap",
            sid,
            len(payload_json),
        )
        return False
    tok_cnt, tok_max, tok_sum = (int(token[0]), int(token[1]), int(token[2]))
    now = int(time.time())
    return _write_cached_payload(
        conn,
        "profile_cache",
        (
            "steamid64",
            "payload_json",
            "token_count",
            "token_max_log_id",
            "token_sum_imported_at",
            "built_at",
        ),
        (sid, payload_json, tok_cnt, tok_max, tok_sum, now),
        f"profile_cache/{sid}",
    )


def get_default_coplayers_cache(
    conn: sqlite3.Connection,
    steamid64: str,
    gamemode: str,
    map_query: str,
    current_token: tuple[int, int, int],
) -> dict[str, Any] | None:
    """Return cached co-players API payload when token matches."""
    sid = (steamid64 or "").strip()
    if not _STEAMID64_RE.fullmatch(sid):
        return None
    gm = (gamemode or "").strip()
    mq = (map_query or "").strip().lower()
    return _read_cached_payload(
        conn,
        "coplayers_cache",
        "steamid64 = ? AND gamemode = ? AND map_query = ?",
        (sid, gm, mq),
        current_token,
        f"coplayers_cache/{sid}",
    )


def set_default_coplayers_cache(
    conn: sqlite3.Connection,
    steamid64: str,
    gamemode: str,
    map_query: str,
    payload: dict[str, Any],
    token: tuple[int, int, int],
) -> bool:
    """Upsert co-players payload for the given scope key."""
    sid = (steamid64 or "").strip()
    if not _STEAMID64_RE.fullmatch(sid) or not isinstance(payload, dict):
        return False
    gm = (gamemode or "").strip()
    mq = (map_query or "").strip().lower()
    try:
        payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError) as e:
        logger.warning("coplayers_cache serialize failed for %s: %s", sid, e)
        return False
    if len(payload_json) > _MAX_PAYLOAD_BYTES:
        logger.warning(
            "coplayers_cache skip store for %s: payload %s bytes exceeds cap",
            sid,
            len(payload_json),
        )
        return False
    tok_cnt, tok_max, tok_sum = (int(token[0]), int(token[1]), int(token[2]))
    now = int(time.time())
    return _write_cached_payload(
        conn,
        "coplayers_cache",
        (
            "steamid64",
            "gamemode",
            "map_query",
            "payload_json",
            "token_count",
            "token_max_log_id",
            "token_sum_imported_at",
            "built_at",
        ),
        (sid, gm, mq, payload_json, tok_cnt, tok_max, tok_sum, now),
        f"coplayers_cache/{sid}",
    )


def _invalidate_table_for_steamids(conn: sqlite3.Connection, table: str, steamid64s: list[str]) -> int:
    uniq = [s for s in dict.fromkeys((x or "").strip() for x in steamid64s) if _STEAMID64_RE.fullmatch(s)]
    if not uniq:
        return 0
    placeholders = ",".join("?" * len(uniq))
    try:
        with _write_lock:
            cur = conn.execute(
                f"DELETE FROM {table} WHERE steamid64 IN ({placeholders})",
                tuple(uniq),
            )
            conn.commit()
        return int(cur.rowcount)
    except sqlite3.Error as e:
        logger.warning("%s invalidate failed: %s", table, e)
        return 0


def invalidate_default_profile_caches(steamid64s: list[str]) -> None:
    """
    Drop disk rows (profile + co-players) and in-memory entries for the given SteamID64s.

    Called when the downloader indexes new stats for those players (proactive invalidation).
    """
    uniq = [s for s in dict.fromkeys((x or "").strip() for x in steamid64s) if _STEAMID64_RE.fullmatch(s)]
    if not uniq:
        return
    from app.search_cache import invalidate_coplayers_for_steamid, invalidate_profile_for_steamid

    for sid in uniq:
        invalidate_profile_for_steamid(sid)
        invalidate_coplayers_for_steamid(sid)
    try:
        conn = connect_profile_cache_db(PROFILE_CACHE_DB_PATH)
        try:
            init_profile_cache_db(conn)
            n_prof = _invalidate_table_for_steamids(conn, "profile_cache", uniq)
            n_cp = _invalidate_table_for_steamids(conn, "coplayers_cache", uniq)
            if n_prof or n_cp:
                logger.debug(
                    "player disk cache invalidated profile=%s coplayers=%s row(s)",
                    n_prof,
                    n_cp,
                )
        finally:
            conn.close()
    except OSError as e:
        logger.warning("player disk cache invalidate could not open DB: %s", e)
