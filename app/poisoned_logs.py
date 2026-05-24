"""Curated blocklist of logs.tf log IDs and uploader SteamID64s excluded from indexing and search."""
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
_MAX_UPLOADERS = 1_000
_LOG_ID_RE = re.compile(r"^\d{1,10}$")
_STEAMID64_RE = re.compile(r"^\d{17}$")

_lock = threading.Lock()
_cached_mtime: float | None = None
_cached_ids: frozenset[int] = frozenset()
_cached_uploaders: frozenset[str] = frozenset()
_cached_uploader_log_ids: frozenset[int] = frozenset()


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


def _parse_uploader_steamid64_payload(data: Any) -> frozenset[str]:
    if not isinstance(data, dict):
        return frozenset()
    raw = data.get("uploader_steamid64")
    if not isinstance(raw, list):
        return frozenset()
    out: set[str] = set()
    for item in raw[: _MAX_UPLOADERS + 1]:
        if item is None:
            continue
        sid = str(item).strip()
        if not _STEAMID64_RE.fullmatch(sid):
            continue
        out.add(sid)
        if len(out) > _MAX_UPLOADERS:
            logger.warning(
                "poisoned uploader_steamid64: more than %s entries; truncating",
                _MAX_UPLOADERS,
            )
            break
    return frozenset(out)


def _fetch_log_ids_for_uploaders(uploaders: frozenset[str]) -> frozenset[int]:
    """Resolve logs.tf log IDs for blocked uploaders (used at purge / cache refresh, not search-time)."""
    if not uploaders:
        return frozenset()
    from app.logs_tf import get_log_list_for_uploader

    out: set[int] = set()
    for sid in sorted(uploaders):
        try:
            out.update(get_log_list_for_uploader(sid))
        except Exception as e:
            logger.warning("poisoned uploader log-id fetch failed for %s: %s", sid, e)
    return frozenset(out)


def _refresh_uploader_log_ids_cache(uploaders: frozenset[str]) -> frozenset[int]:
    ids = _fetch_log_ids_for_uploaders(uploaders)
    global _cached_uploader_log_ids
    with _lock:
        _cached_uploader_log_ids = ids
    if ids:
        logger.info(
            "Poisoned uploader cache refreshed: %s uploader(s), %s log id(s)",
            len(uploaders),
            len(ids),
        )
    return ids


def _schedule_uploader_log_id_refresh(uploaders: frozenset[str]) -> None:
    if not uploaders:
        return

    def _run() -> None:
        try:
            _refresh_uploader_log_ids_cache(uploaders)
        except Exception:
            logger.exception("Background poisoned uploader log-id refresh failed")

    threading.Thread(
        target=_run,
        name="poisoned-uploader-log-ids",
        daemon=True,
    ).start()


def _load_blocklist_from_disk() -> tuple[frozenset[int], frozenset[str]]:
    path = Path(POISONED_LOG_IDS_PATH)
    try:
        if not path.is_file():
            return frozenset(), frozenset()
        size = path.stat().st_size
        if size > _MAX_FILE_BYTES:
            logger.warning("poisoned_log_ids file too large (%s bytes); ignoring", size)
            return frozenset(), frozenset()
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        logger.warning("poisoned_log_ids read failed (%s): %s", path, e)
        return frozenset(), frozenset()
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        logger.warning("poisoned_log_ids JSON invalid (%s): %s", path, e)
        return frozenset(), frozenset()
    ids = _parse_log_ids_payload(data)
    uploaders = _parse_uploader_steamid64_payload(data)
    if ids or uploaders:
        logger.debug(
            "poisoned blocklist loaded log_ids=%s uploader_steamid64=%s from %s",
            len(ids),
            len(uploaders),
            path,
        )
    return ids, uploaders


def _reload_blocklist_if_needed() -> None:
    global _cached_mtime, _cached_ids, _cached_uploaders, _cached_uploader_log_ids
    path = Path(POISONED_LOG_IDS_PATH)
    try:
        mtime = path.stat().st_mtime if path.is_file() else -1.0
    except OSError:
        mtime = -1.0
    with _lock:
        if mtime == _cached_mtime:
            return
        prev_uploaders = _cached_uploaders
        _cached_ids, _cached_uploaders = _load_blocklist_from_disk()
        _cached_mtime = mtime
        if _cached_uploaders != prev_uploaders:
            _cached_uploader_log_ids = frozenset()
        uploaders_to_refresh = _cached_uploaders
    if uploaders_to_refresh:
        _schedule_uploader_log_id_refresh(uploaders_to_refresh)


def poisoned_log_ids() -> frozenset[int]:
    """Return log IDs listed explicitly in the blocklist file."""
    _reload_blocklist_if_needed()
    with _lock:
        return _cached_ids


def poisoned_uploader_steamid64s() -> frozenset[str]:
    """Return uploader SteamID64s whose logs are excluded (reloads on file mtime change)."""
    _reload_blocklist_if_needed()
    with _lock:
        return _cached_uploaders


def excluded_log_ids() -> frozenset[int]:
    """
    All log IDs excluded from search/SQL: explicit ``log_ids`` plus logs.tf IDs for blocked uploaders.

    Uploader SteamID64s are resolved to log IDs at purge/refresh time — search only filters by log_id.
    """
    _reload_blocklist_if_needed()
    with _lock:
        uploaders = _cached_uploaders
        ids = _cached_ids
        uploader_ids = _cached_uploader_log_ids
    if uploaders and not uploader_ids:
        uploader_ids = _refresh_uploader_log_ids_cache(uploaders)
    return ids | uploader_ids


def extract_uploader_steamid64(logtext: dict[str, Any] | None) -> str | None:
    """Parse logs.tf ``info.uploader`` (object or string) to a 17-digit SteamID64."""
    if not isinstance(logtext, dict):
        return None
    info = logtext.get("info")
    if not isinstance(info, dict):
        return None
    raw = info.get("uploader")
    if isinstance(raw, dict):
        raw = raw.get("id")
    if raw is None:
        return None
    sid = str(raw).strip()
    if _STEAMID64_RE.fullmatch(sid):
        return sid
    return None


def is_poisoned_uploader(steamid64: str | None) -> bool:
    if not steamid64:
        return False
    sid = str(steamid64).strip()
    if not _STEAMID64_RE.fullmatch(sid):
        return False
    return sid in poisoned_uploader_steamid64s()


def is_log_excluded(
    log_id: int | str | None,
    logtext: dict[str, Any] | None = None,
    *,
    uploader_steamid64: str | None = None,
) -> bool:
    """
    True when a log must not be indexed or searched.

    Uses the merged excluded log-id set when ``log_id`` is known; at ingest time also checks
    ``info.uploader`` so new uploads are rejected before they enter the DB.
    """
    if log_id is not None:
        try:
            lid = int(log_id)
        except (TypeError, ValueError):
            lid = None
        else:
            if lid in excluded_log_ids():
                return True
    uploaders = poisoned_uploader_steamid64s()
    if not uploaders:
        return False
    sid = (uploader_steamid64 or "").strip() or None
    if sid is None and logtext is not None:
        sid = extract_uploader_steamid64(logtext)
    return bool(sid and sid in uploaders)


def is_poisoned(log_id: int | str | None) -> bool:
    """True when ``log_id`` is excluded (explicit blocklist or a blocked uploader's log)."""
    if log_id is None:
        return False
    try:
        lid = int(log_id)
    except (TypeError, ValueError):
        return False
    return lid in excluded_log_ids()


def _validated_table_alias(alias: str, *, default: str = "l") -> str:
    a = (alias or default).strip() or default
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", a):
        return default
    return a


def poisoned_log_exclusion_sql(table_alias: str = "l") -> str:
    """
    SQL fragment `` AND alias.log_id NOT IN (...)`` for all excluded log IDs.

    Parameter-free (IDs are validated integers). Uploader rules are pre-resolved to log IDs.
    """
    alias = _validated_table_alias(table_alias)
    ids = sorted(excluded_log_ids())
    if not ids:
        return ""
    id_list = ",".join(str(i) for i in ids)
    return f" AND {alias}.log_id NOT IN ({id_list})"


def poisoned_log_id_column_sql(column: str = "log_id") -> str:
    """SQL fragment `` AND column NOT IN (...)`` for a bare ``log_id`` column reference."""
    col = (column or "log_id").strip() or "log_id"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]*", col):
        return ""
    ids = sorted(excluded_log_ids())
    if not ids:
        return ""
    id_list = ",".join(str(i) for i in ids)
    return f" AND {col} NOT IN ({id_list})"


def _collect_log_ids_to_purge() -> set[int]:
    """Log IDs to purge: explicit list plus all logs.tf IDs for blocked uploaders."""
    ids = set(poisoned_log_ids())
    uploaders = poisoned_uploader_steamid64s()
    if uploaders:
        ids.update(_fetch_log_ids_for_uploaders(uploaders))
        _refresh_uploader_log_ids_cache(uploaders)
    return ids


def purge_poisoned_from_chat_db(conn: sqlite3.Connection) -> int:
    """Remove poisoned logs from chat DB tables. Returns rows deleted from chat_logs."""
    ids = sorted(_collect_log_ids_to_purge())
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
    ids = sorted(_collect_log_ids_to_purge())
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
    Purge blocklisted logs from chat and stats SQLite DBs (best-effort).

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
            "Purged poisoned logs: chat_logs=%s stats_logs=%s "
            "(blocklist log_ids=%s uploader_steamid64=%s)",
            n_chat,
            n_stats,
            len(poisoned_log_ids()),
            len(poisoned_uploader_steamid64s()),
        )
    return n_chat, n_stats
