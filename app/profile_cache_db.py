"""Disk cache for default (unfiltered) player profile payloads."""
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
# Bound worst-case blob size (DoS / accidental huge profiles).
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
    cur_cnt, cur_max, cur_sum = (int(current_token[0]), int(current_token[1]), int(current_token[2]))
    try:
        row = conn.execute(
            """
            SELECT payload_json, token_count, token_max_log_id, token_sum_imported_at, built_at
            FROM profile_cache
            WHERE steamid64 = ?
            """,
            (sid,),
        ).fetchone()
    except sqlite3.Error as e:
        logger.warning("profile_cache read failed for %s: %s", sid, e)
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
            logger.warning("profile_cache row too large for %s (%s bytes)", sid, len(payload_json))
            return None
        data = json.loads(payload_json)
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning("profile_cache JSON invalid for %s: %s", sid, e)
        return None
    if not isinstance(data, dict):
        return None
    return data


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
    try:
        with _write_lock:
            conn.execute(
                """
                INSERT OR REPLACE INTO profile_cache (
                  steamid64, payload_json, token_count, token_max_log_id,
                  token_sum_imported_at, built_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (sid, payload_json, tok_cnt, tok_max, tok_sum, now),
            )
            conn.commit()
        return True
    except sqlite3.Error as e:
        logger.warning("profile_cache write failed for %s: %s", sid, e)
        return False


def _invalidate_default_profile_cache_conn(conn: sqlite3.Connection, steamid64s: list[str]) -> int:
    uniq = [s for s in dict.fromkeys((x or "").strip() for x in steamid64s) if _STEAMID64_RE.fullmatch(s)]
    if not uniq:
        return 0
    placeholders = ",".join("?" * len(uniq))
    try:
        with _write_lock:
            cur = conn.execute(
                f"DELETE FROM profile_cache WHERE steamid64 IN ({placeholders})",
                tuple(uniq),
            )
            conn.commit()
        return int(cur.rowcount)
    except sqlite3.Error as e:
        logger.warning("profile_cache invalidate failed: %s", e)
        return 0


def invalidate_default_profile_caches(steamid64s: list[str]) -> None:
    """
    Drop disk rows and in-memory profile entries for the given SteamID64s.

    Called when the downloader indexes new stats for those players (proactive invalidation).
    """
    uniq = [s for s in dict.fromkeys((x or "").strip() for x in steamid64s) if _STEAMID64_RE.fullmatch(s)]
    if not uniq:
        return
    from app.search_cache import invalidate_profile_for_steamid

    for sid in uniq:
        invalidate_profile_for_steamid(sid)
    try:
        conn = connect_profile_cache_db(PROFILE_CACHE_DB_PATH)
        try:
            init_profile_cache_db(conn)
            n = _invalidate_default_profile_cache_conn(conn, uniq)
            if n:
                logger.debug("profile_cache invalidated %s row(s)", n)
        finally:
            conn.close()
    except OSError as e:
        logger.warning("profile_cache invalidate could not open DB: %s", e)
