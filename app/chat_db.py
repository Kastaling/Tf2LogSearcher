"""SQLite storage for chat messages extracted from logs.tf JSON logs."""
from __future__ import annotations

import logging
import re
import sqlite3
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)
_STEAMID64_OFFSET = 76561197960265728
_STEAMID3_RE = re.compile(r"^\[U:1:(\d+)\]$")


def connect_chat_db(db_path: str | Path) -> sqlite3.Connection:
    """
    Open/create chat SQLite DB with pragmatic settings for read-heavy workloads.

    WAL allows concurrent reads while downloader writes.
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _init_fts_if_available(conn: sqlite3.Connection) -> None:
    """Create optional FTS5 index for fast word lookups if the build supports it."""
    try:
        conn.executescript(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS chat_messages_fts
            USING fts5(msg, content='chat_messages', content_rowid='id');

            CREATE TRIGGER IF NOT EXISTS chat_messages_ai AFTER INSERT ON chat_messages BEGIN
              INSERT INTO chat_messages_fts(rowid, msg) VALUES (new.id, new.msg);
            END;

            CREATE TRIGGER IF NOT EXISTS chat_messages_ad AFTER DELETE ON chat_messages BEGIN
              INSERT INTO chat_messages_fts(chat_messages_fts, rowid, msg) VALUES('delete', old.id, old.msg);
            END;

            CREATE TRIGGER IF NOT EXISTS chat_messages_au AFTER UPDATE ON chat_messages BEGIN
              INSERT INTO chat_messages_fts(chat_messages_fts, rowid, msg) VALUES('delete', old.id, old.msg);
              INSERT INTO chat_messages_fts(rowid, msg) VALUES (new.id, new.msg);
            END;
            """
        )
    except sqlite3.OperationalError as e:
        # Some SQLite builds might not include FTS5; base table/indexes still work.
        logger.warning("FTS5 unavailable for chat DB; continuing without full-text index: %s", e)


def init_chat_db(conn: sqlite3.Connection) -> None:
    """Create schema + indexes (idempotent)."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS chat_logs (
          log_id INTEGER PRIMARY KEY,
          log_date_ts INTEGER,
          map TEXT NOT NULL DEFAULT '',
          imported_at_ts INTEGER NOT NULL,
          chat_count INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS chat_messages (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id INTEGER NOT NULL,
          message_idx INTEGER NOT NULL,
          steamid3 TEXT NOT NULL DEFAULT '',
          steamid64 TEXT,
          alias TEXT NOT NULL DEFAULT '',
          team TEXT CHECK (team IN ('Red', 'Blue') OR team IS NULL),
          msg TEXT NOT NULL,
          UNIQUE(log_id, message_idx),
          FOREIGN KEY(log_id) REFERENCES chat_logs(log_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_chat_messages_log_id ON chat_messages(log_id);
        CREATE INDEX IF NOT EXISTS idx_chat_messages_steamid3 ON chat_messages(steamid3);
        CREATE INDEX IF NOT EXISTS idx_chat_messages_steamid64 ON chat_messages(steamid64);
        CREATE INDEX IF NOT EXISTS idx_chat_logs_log_date_ts ON chat_logs(log_date_ts);
        """
    )
    _init_fts_if_available(conn)


def _team_from_players(players: Any, steamid3: str) -> str | None:
    if not isinstance(players, dict):
        return None
    p = players.get(steamid3)
    if not isinstance(p, dict):
        return None
    t = p.get("team")
    if t == "Red":
        return "Red"
    if t == "Blue":
        return "Blue"
    return None


def _steamid3_to_steamid64(steamid3: str) -> str | None:
    m = _STEAMID3_RE.match((steamid3 or "").strip())
    if not m:
        return None
    try:
        return str(_STEAMID64_OFFSET + int(m.group(1)))
    except ValueError:
        return None


def _steamid64_to_steamid3(steamid64: str | int) -> str | None:
    try:
        a = int(steamid64) - _STEAMID64_OFFSET
    except (TypeError, ValueError):
        return None
    if a < 0:
        return None
    return f"[U:1:{a}]"


def local_chat_log_ids_for_player(steamid64: str, db_path: str | Path) -> frozenset[int]:
    """
    Distinct log IDs in chat DB where the player has chat rows.

    Used by chat cache invalidation to avoid expensive logs.tf + filesystem scans.
    """
    steamid3 = _steamid64_to_steamid3(steamid64)
    if not steamid3:
        return frozenset()
    path = Path(db_path)
    if not path.is_file():
        return frozenset()
    conn = sqlite3.connect(str(path), timeout=30)
    try:
        rows = conn.execute(
            "SELECT DISTINCT log_id FROM chat_messages WHERE steamid3 = ?",
            (steamid3,),
        ).fetchall()
    finally:
        conn.close()
    return frozenset(int(r[0]) for r in rows if r and r[0] is not None)


def _extract_chat_rows(log_id: int, logtext: dict[str, Any]) -> tuple[int | None, str, list[tuple[Any, ...]]]:
    """Return (log_date_ts, map_name, rows_for_chat_messages)."""
    info = logtext.get("info") if isinstance(logtext, dict) else None
    if not isinstance(info, dict):
        info = {}
    try:
        log_date_ts = int(info.get("date")) if info.get("date") is not None else None
    except (TypeError, ValueError):
        log_date_ts = None
    map_name = str(info.get("map") or "").strip()

    chat = logtext.get("chat") if isinstance(logtext, dict) else None
    if not isinstance(chat, list) or not chat:
        return log_date_ts, map_name, []
    players = logtext.get("players")

    rows: list[tuple[Any, ...]] = []
    for idx, entry in enumerate(chat):
        if not isinstance(entry, dict):
            continue
        raw_msg = entry.get("msg")
        if raw_msg is None:
            continue
        msg = str(raw_msg)
        if not msg:
            continue
        steamid3 = str(entry.get("steamid") or "").strip()
        steamid64 = _steamid3_to_steamid64(steamid3)
        alias = str(entry.get("name") or "").strip()
        team = _team_from_players(players, steamid3)
        rows.append((log_id, idx, steamid3, steamid64, alias, team, msg))
    return log_date_ts, map_name, rows


def replace_chat_for_log(
    conn: sqlite3.Connection,
    log_id: int,
    logtext: dict[str, Any],
    *,
    imported_at_ts: int | None = None,
) -> int:
    """
    Replace all chat rows for one log in a single DB transaction.

    Caller controls transaction scope. Returns number of inserted chat rows.
    """
    ts = int(time.time()) if imported_at_ts is None else int(imported_at_ts)
    log_date_ts, map_name, rows = _extract_chat_rows(log_id, logtext)

    conn.execute("DELETE FROM chat_messages WHERE log_id = ?", (log_id,))
    conn.execute("DELETE FROM chat_logs WHERE log_id = ?", (log_id,))
    conn.execute(
        "INSERT INTO chat_logs (log_id, log_date_ts, map, imported_at_ts, chat_count) VALUES (?, ?, ?, ?, ?)",
        (log_id, log_date_ts, map_name, ts, len(rows)),
    )
    if rows:
        conn.executemany(
            """
            INSERT INTO chat_messages (
              log_id, message_idx, steamid3, steamid64, alias, team, msg
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)
