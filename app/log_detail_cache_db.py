"""Permanent section cache for built-in log detail pages."""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from app.config import LOG_DETAIL_CACHE_DB_PATH

logger = logging.getLogger(__name__)

_MAX_PAYLOAD_BYTES = 8_388_608  # 8 MiB per section
_write_lock = threading.Lock()


def connect_log_detail_cache_db(db_path: str | Path | None = None) -> sqlite3.Connection:
    path = Path(db_path or LOG_DETAIL_CACHE_DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30.0)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_log_detail_cache_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS log_detail_sections (
          log_id INTEGER NOT NULL,
          section_key TEXT NOT NULL,
          section_version TEXT NOT NULL,
          source_fingerprint TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          built_at INTEGER NOT NULL,
          PRIMARY KEY (log_id, section_key)
        );
        CREATE INDEX IF NOT EXISTS idx_log_detail_sections_fingerprint
          ON log_detail_sections(log_id, source_fingerprint);
        """
    )
    conn.commit()


def get_cached_section(
    conn: sqlite3.Connection,
    log_id: int,
    section_key: str,
    section_version: str,
    source_fingerprint: str,
) -> dict[str, Any] | list[Any] | None:
    try:
        row = conn.execute(
            """
            SELECT payload_json, section_version, source_fingerprint
            FROM log_detail_sections
            WHERE log_id = ? AND section_key = ?
            """,
            (int(log_id), section_key),
        ).fetchone()
    except sqlite3.Error as e:
        logger.warning("log_detail_cache read %s/%s failed: %s", log_id, section_key, e)
        return None
    if not row:
        return None
    payload_json, cached_version, cached_fp = row
    if cached_version != section_version or cached_fp != source_fingerprint:
        return None
    if not payload_json or len(payload_json) > _MAX_PAYLOAD_BYTES:
        return None
    try:
        return json.loads(payload_json)
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning("log_detail_cache JSON invalid %s/%s: %s", log_id, section_key, e)
        return None


def set_cached_section(
    conn: sqlite3.Connection,
    log_id: int,
    section_key: str,
    section_version: str,
    source_fingerprint: str,
    payload: Any,
) -> bool:
    try:
        payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError) as e:
        logger.warning("log_detail_cache serialize %s/%s: %s", log_id, section_key, e)
        return False
    if len(payload_json) > _MAX_PAYLOAD_BYTES:
        logger.warning(
            "log_detail_cache skip %s/%s: %s bytes exceeds cap",
            log_id,
            section_key,
            len(payload_json),
        )
        return False
    built_at = int(time.time())
    try:
        with _write_lock:
            conn.execute(
                """
                INSERT OR REPLACE INTO log_detail_sections (
                  log_id, section_key, section_version, source_fingerprint,
                  payload_json, built_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    int(log_id),
                    section_key,
                    section_version,
                    source_fingerprint,
                    payload_json,
                    built_at,
                ),
            )
            conn.commit()
        return True
    except sqlite3.Error as e:
        logger.warning("log_detail_cache write %s/%s failed: %s", log_id, section_key, e)
        return False


def delete_sections_for_log(conn: sqlite3.Connection, log_id: int) -> None:
    with _write_lock:
        conn.execute("DELETE FROM log_detail_sections WHERE log_id = ?", (int(log_id),))
        conn.commit()
