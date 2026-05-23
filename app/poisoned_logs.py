"""Curated blocklist of logs.tf log IDs excluded from all indexing and search."""
from __future__ import annotations

import json
import logging
import re
import sqlite3
import threading
from pathlib import Path
from typing import Any

from app.config import POISONED_LOG_IDS_PATH

logger = logging.getLogger(__name__)

_MAX_FILE_BYTES = 1_048_576  # 1 MiB
_MAX_LOG_IDS = 10_000
_LOG_ID_RE = re.compile(r"^\d{1,10}$")

_lock = threading.Lock()
_cached_mtime: float | None = None
_cached_ids: frozenset[int] = frozenset()


def _parse_log_ids_payload(data: Any) -> frozenset[int]:
    if not isinstance(data, dict):
        return frozenset()
    raw = data.get("log_ids")
    if not isinstance(raw, list):
        return frozenset()
    out: set[int] = set()
    for item in raw[: _MAX_LOG_IDS + 1]:
        if isinstance(item, bool):
            continue
        try:
            lid = int(item)
        except (TypeError, ValueError):
            continue
        if lid <= 0 or not _LOG_ID_RE.fullmatch(str(lid)):
            continue
        out.add(lid)
        if len(out) > _MAX_LOG_IDS:
            logger.warning(
                "poisoned_log_ids: more than %s entries; truncating",
                _MAX_LOG_IDS,
            )
            break
    return frozenset(out)


def _load_from_disk() -> frozenset[int]:
    path = Path(POISONED_LOG_IDS_PATH)
    try:
        if not path.is_file():
            return frozenset()
        size = path.stat().st_size
        if size > _MAX_FILE_BYTES:
            logger.warning("poisoned_log_ids file too large (%s bytes); ignoring", size)
            return frozenset()
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        logger.warning("poisoned_log_ids read failed (%s): %s", path, e)
        return frozenset()
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        logger.warning("poisoned_log_ids JSON invalid (%s): %s", path, e)
        return frozenset()
    ids = _parse_log_ids_payload(data)
    if ids:
        logger.debug("poisoned_log_ids loaded %s id(s) from %s", len(ids), path)
    return ids


def poisoned_log_ids() -> frozenset[int]:
    """Return the current blocklist (reloads when the file mtime changes)."""
    global _cached_mtime, _cached_ids
    path = Path(POISONED_LOG_IDS_PATH)
    try:
        mtime = path.stat().st_mtime if path.is_file() else -1.0
    except OSError:
        mtime = -1.0
    with _lock:
        if mtime == _cached_mtime:
            return _cached_ids
        _cached_ids = _load_from_disk()
        _cached_mtime = mtime
        return _cached_ids


def is_poisoned(log_id: int | str | None) -> bool:
    if log_id is None:
        return False
    try:
        lid = int(log_id)
    except (TypeError, ValueError):
        return False
    return lid in poisoned_log_ids()


def poisoned_log_exclusion_sql(table_alias: str = "l") -> str:
    """
    SQL fragment `` AND alias.log_id NOT IN (...)`` for curated poisoned IDs.

    Parameter-free (IDs are validated integers from the JSON file).
    Returns an empty string when the blocklist is empty.
    """
    alias = (table_alias or "l").strip() or "l"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", alias):
        return ""
    return poisoned_log_id_column_sql(f"{alias}.log_id")


def poisoned_log_id_column_sql(column: str = "log_id") -> str:
    """SQL fragment `` AND column NOT IN (...)`` for a bare ``log_id`` column reference."""
    col = (column or "log_id").strip() or "log_id"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]*", col):
        return ""
    ids = sorted(poisoned_log_ids())
    if not ids:
        return ""
    id_list = ",".join(str(i) for i in ids)
    return f" AND {col} NOT IN ({id_list})"


def purge_poisoned_from_chat_db(conn: sqlite3.Connection) -> int:
    """Remove poisoned logs from chat DB tables. Returns rows deleted from chat_logs."""
    ids = sorted(poisoned_log_ids())
    if not ids:
        return 0
    placeholders = ",".join("?" * len(ids))
    try:
        conn.execute(f"DELETE FROM chat_messages WHERE log_id IN ({placeholders})", tuple(ids))
        cur = conn.execute(f"DELETE FROM chat_logs WHERE log_id IN ({placeholders})", tuple(ids))
        return int(cur.rowcount)
    except sqlite3.Error as e:
        logger.warning("poisoned chat purge failed: %s", e)
        return 0


def purge_poisoned_from_stats_db(conn: sqlite3.Connection) -> int:
    """
    Remove poisoned logs from stats DB (CASCADE child rows) and refresh affected aggregates.

    Returns number of logs removed from ``logs``.
    """
    ids = sorted(poisoned_log_ids())
    if not ids:
        return 0
    placeholders = ",".join("?" * len(ids))
    try:
        rows = conn.execute(
            f"SELECT DISTINCT steamid64 FROM log_players WHERE log_id IN ({placeholders})",
            tuple(ids),
        ).fetchall()
        steamids = [str(r[0]) for r in rows if r and r[0]]
        cur = conn.execute(f"DELETE FROM logs WHERE log_id IN ({placeholders})", tuple(ids))
        n = int(cur.rowcount)
        if steamids:
            from app.stats_db import flush_player_stats_agg

            flush_player_stats_agg(conn, steamids)
        return n
    except sqlite3.Error as e:
        logger.warning("poisoned stats purge failed: %s", e)
        return 0


def purge_poisoned_logs(
    *,
    chat_db_path: str | Path | None = None,
    stats_db_path: str | Path | None = None,
) -> tuple[int, int]:
    """
    Purge poisoned log IDs from chat and stats SQLite DBs (best-effort).

    Returns ``(chat_logs_removed, stats_logs_removed)``.
    """
    from app.config import CHAT_DB_PATH, STATS_DB_PATH

    chat_p = Path(chat_db_path or CHAT_DB_PATH)
    stats_p = Path(stats_db_path or STATS_DB_PATH)
    n_chat = 0
    n_stats = 0
    if chat_p.is_file():
        try:
            conn = sqlite3.connect(str(chat_p), timeout=30.0)
            try:
                conn.execute("PRAGMA busy_timeout=30000")
                n_chat = purge_poisoned_from_chat_db(conn)
                conn.commit()
            finally:
                conn.close()
        except OSError as e:
            logger.warning("poisoned chat purge could not open %s: %s", chat_p, e)
    if stats_p.is_file():
        try:
            conn = sqlite3.connect(str(stats_p), timeout=30.0)
            try:
                conn.execute("PRAGMA busy_timeout=30000")
                n_stats = purge_poisoned_from_stats_db(conn)
                conn.commit()
            finally:
                conn.close()
        except OSError as e:
            logger.warning("poisoned stats purge could not open %s: %s", stats_p, e)
    if n_chat or n_stats:
        logger.info(
            "Purged poisoned logs: chat_logs=%s stats_logs=%s (blocklist size=%s)",
            n_chat,
            n_stats,
            len(poisoned_log_ids()),
        )
    return n_chat, n_stats
