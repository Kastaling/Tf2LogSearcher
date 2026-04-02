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
# Set in chat_app_meta after a full alias FTS rebuild; player-name search uses FTS only when value is "1".
CHAT_ALIAS_FTS_READY_META_KEY = "alias_fts_ready"
_ALIAS_FTS_REBUILD_ATTEMPTS = 90
_ALIAS_FTS_REBUILD_SLEEP_SEC = 2.0
# Downloader retries pending rebuild once per cycle; cap busy retries so each cycle does not stall minutes.
ALIAS_FTS_CYCLE_BUSY_ATTEMPTS = 20
# Progress handler: invoke every N VM instructions; throttle logs to this wall-clock interval.
_ALIAS_FTS_PROGRESS_OPCODE_INTERVAL = 2_000_000
ALIAS_FTS_PROGRESS_HEARTBEAT_SEC = 15.0


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


def _alias_fts_mark_ready(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO chat_app_meta(key, value) VALUES(?, ?)",
        (CHAT_ALIAS_FTS_READY_META_KEY, "1"),
    )


def alias_fts_rebuild_pending(conn: sqlite3.Connection) -> bool:
    """True if chat_app_meta does not mark the alias trigram index as ready."""
    try:
        row = conn.execute(
            "SELECT value FROM chat_app_meta WHERE key = ? LIMIT 1",
            (CHAT_ALIAS_FTS_READY_META_KEY,),
        ).fetchone()
    except sqlite3.OperationalError:
        row = None
    return row is None or row[0] != "1"


def _execute_alias_fts_rebuild_insert(conn: sqlite3.Connection, *, log_progress: bool) -> None:
    """Run FTS5 external-content rebuild; optional SQLite progress-handler heartbeats."""
    if not log_progress:
        conn.execute(
            "INSERT INTO chat_messages_alias_fts(chat_messages_alias_fts) VALUES('rebuild')"
        )
        return

    last_mono: list[float] = [0.0]

    def on_progress() -> int:
        now = time.monotonic()
        if now - last_mono[0] >= ALIAS_FTS_PROGRESS_HEARTBEAT_SEC:
            last_mono[0] = now
            logger.info(
                "Alias FTS: rebuild still running (SQLite VM active — large databases can take tens of minutes)"
            )
        return 0

    conn.set_progress_handler(on_progress, _ALIAS_FTS_PROGRESS_OPCODE_INTERVAL)
    try:
        conn.execute(
            "INSERT INTO chat_messages_alias_fts(chat_messages_alias_fts) VALUES('rebuild')"
        )
    finally:
        # Some sqlite3 builds require n even when clearing the handler (handler=None).
        conn.set_progress_handler(None, 0)


def _maybe_rebuild_alias_fts(
    conn: sqlite3.Connection,
    *,
    log_progress: bool = False,
    busy_attempts: int | None = None,
) -> None:
    """
    Ensure alias trigram FTS is fully populated and mark ready in chat_app_meta.

    Retries on SQLITE_BUSY / locked (e.g. another container briefly holds the DB).
    Must not be called from the web app while the downloader writes continuously —
    use the downloader or backfill process to run init_chat_db.

    busy_attempts: max BEGIN IMMEDIATE retries; default is _ALIAS_FTS_REBUILD_ATTEMPTS.
    """
    if not alias_fts_rebuild_pending(conn):
        return

    attempts_total = (
        busy_attempts if busy_attempts is not None else _ALIAS_FTS_REBUILD_ATTEMPTS
    )
    attempts_total = max(1, min(attempts_total, _ALIAS_FTS_REBUILD_ATTEMPTS))

    for attempt in range(attempts_total):
        try:
            conn.execute("BEGIN IMMEDIATE")
            n_msgs = int(conn.execute("SELECT COUNT(*) FROM chat_messages").fetchone()[0])
            if n_msgs == 0:
                _alias_fts_mark_ready(conn)
            else:
                logger.info(
                    "Alias FTS: starting INSERT rebuild over %s chat_messages rows (single long transaction)...",
                    n_msgs,
                )
                _execute_alias_fts_rebuild_insert(conn, log_progress=log_progress)
                logger.info(
                    "Alias FTS: INSERT rebuild step finished for %s source rows",
                    n_msgs,
                )
                _alias_fts_mark_ready(conn)
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            try:
                conn.rollback()
            except sqlite3.OperationalError:
                pass
            err = str(e).lower()
            if "locked" not in err and "busy" not in err:
                logger.warning("chat_messages_alias_fts rebuild failed: %s", e)
                return
            if attempt + 1 >= attempts_total:
                logger.warning(
                    "chat_messages_alias_fts rebuild skipped after %s attempts: %s",
                    attempts_total,
                    e,
                )
                return
            logger.info(
                "Alias FTS: database busy (attempt %s/%s); retrying in %ss...",
                attempt + 1,
                attempts_total,
                _ALIAS_FTS_REBUILD_SLEEP_SEC,
            )
            time.sleep(_ALIAS_FTS_REBUILD_SLEEP_SEC)
        except Exception:
            try:
                conn.rollback()
            except sqlite3.OperationalError:
                pass
            raise


def run_alias_fts_rebuild_if_needed(
    conn: sqlite3.Connection,
    *,
    log_progress: bool = False,
    busy_attempts: int | None = None,
) -> None:
    """
    Blocking alias FTS rebuild on an existing connection (single writer).

    Use the downloader's shared chat connection so no second handle contends for the DB.
    """
    _maybe_rebuild_alias_fts(conn, log_progress=log_progress, busy_attempts=busy_attempts)


def rebuild_alias_fts_if_needed(
    db_path: str | Path,
    *,
    log_progress: bool = True,
    busy_attempts: int | None = None,
) -> None:
    """
    Open the chat DB and run rebuild when the ready flag is unset.

    Used by backfill, `python -m app.rebuild_alias_fts`, and one-off maintenance.
    """
    path = Path(db_path)
    if not path.is_file():
        return
    conn = connect_chat_db(path)
    try:
        _maybe_rebuild_alias_fts(
            conn, log_progress=log_progress, busy_attempts=busy_attempts
        )
    finally:
        conn.close()


def _init_alias_trigram_fts(conn: sqlite3.Connection) -> None:
    """
    FTS5 trigram index on chat alias for fast substring player-name search.

    Without this, instr(lower(alias), ?) scans the entire chat_messages table.
    """
    try:
        conn.executescript(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS chat_messages_alias_fts
            USING fts5(
              alias,
              tokenize = 'trigram',
              content='chat_messages',
              content_rowid='id'
            );

            CREATE TRIGGER IF NOT EXISTS chat_messages_ai_alias AFTER INSERT ON chat_messages BEGIN
              INSERT INTO chat_messages_alias_fts(rowid, alias) VALUES (new.id, new.alias);
            END;

            CREATE TRIGGER IF NOT EXISTS chat_messages_ad_alias AFTER DELETE ON chat_messages BEGIN
              INSERT INTO chat_messages_alias_fts(chat_messages_alias_fts, rowid, alias)
                VALUES('delete', old.id, old.alias);
            END;

            CREATE TRIGGER IF NOT EXISTS chat_messages_au_alias AFTER UPDATE ON chat_messages BEGIN
              INSERT INTO chat_messages_alias_fts(chat_messages_alias_fts, rowid, alias)
                VALUES('delete', old.id, old.alias);
              INSERT INTO chat_messages_alias_fts(rowid, alias) VALUES (new.id, new.alias);
            END;
            """
        )
    except sqlite3.OperationalError as e:
        logger.warning(
            "FTS5 trigram alias index unavailable (older SQLite or no FTS5); "
            "player-name search may be slow: %s",
            e,
        )
        return

    # Do not rebuild here: full FTS rebuild can take a long time. The downloader runs it on its
    # shared connection before downloads; use rebuild_alias_fts_if_needed() for CLI/backfill.


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

        CREATE TABLE IF NOT EXISTS chat_app_meta (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        """
    )
    _init_fts_if_available(conn)
    _init_alias_trigram_fts(conn)


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


def local_all_chat_log_ids(db_path: str | Path) -> frozenset[int]:
    """All log IDs currently present in chat DB."""
    path = Path(db_path)
    if not path.is_file():
        return frozenset()
    conn = sqlite3.connect(str(path), timeout=30)
    try:
        rows = conn.execute("SELECT log_id FROM chat_logs").fetchall()
    finally:
        conn.close()
    return frozenset(int(r[0]) for r in rows if r and r[0] is not None)


def chat_log_fingerprint(db_path: str | Path) -> frozenset[int]:
    """
    Lightweight cache fingerprint for chat DB contents.

    Encodes (row_count, max_log_id) as a frozenset for compatibility with cache API.
    """
    path = Path(db_path)
    if not path.is_file():
        return frozenset()
    conn = sqlite3.connect(str(path), timeout=30)
    try:
        row = conn.execute(
            "SELECT COUNT(*), COALESCE(MAX(log_id), 0) FROM chat_logs"
        ).fetchone()
    finally:
        conn.close()
    count = int(row[0] or 0) if row else 0
    max_id = int(row[1] or 0) if row else 0
    return frozenset((count, max_id))


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