"""Per-profile page view counters (total + unique visitors)."""
from __future__ import annotations

import hashlib
import hmac
import sqlite3
import threading
import time
from pathlib import Path

from app.config import PROFILE_VIEW_HASH_SECRET, PROFILE_VIEWS_DB_PATH

_lock = threading.Lock()
_conn: sqlite3.Connection | None = None


def _connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30.0)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _get_conn() -> sqlite3.Connection:
    """Return the module-level connection, initialising it on first call."""
    global _conn
    if _conn is not None:
        return _conn
    with _lock:
        if _conn is not None:  # double-checked locking
            return _conn
        path = Path(PROFILE_VIEWS_DB_PATH)
        c = _connect(path)
        init_profile_views_db(c)
        _conn = c
    return _conn


def init_profile_views_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS profile_view_totals (
          steamid64       TEXT PRIMARY KEY,
          total_views     INTEGER NOT NULL DEFAULT 0,
          unique_visitors INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS profile_view_visitors (
          steamid64   TEXT NOT NULL,
          visitor_id  TEXT NOT NULL,
          first_seen  INTEGER NOT NULL,
          PRIMARY KEY (steamid64, visitor_id)
        );
        CREATE INDEX IF NOT EXISTS idx_profile_visitors_steam ON profile_view_visitors(steamid64);
        """
    )
    conn.commit()


def visitor_fingerprint(client_ip: str, user_agent: str | None) -> str:
    """Stable, non-reversible identifier for a coarse visitor bucket (IP + UA)."""
    ip = (client_ip or "").strip()[:128]
    ua = (user_agent or "").strip()[:400]
    payload = f"{ip}\n{ua}".encode("utf-8", errors="replace")
    secret = PROFILE_VIEW_HASH_SECRET
    if secret:
        digest = hmac.new(secret, payload, hashlib.sha256).hexdigest()
    else:
        digest = hashlib.sha256(payload).hexdigest()
    return digest[:32]


def record_profile_view(steamid64: str, visitor_id: str) -> tuple[int, int]:
    """
    Increment totals for a successful profile view. Returns (total_views, unique_visitors).

    ``visitor_id`` must be ASCII (hex from ``visitor_fingerprint``).
    """
    sid = (steamid64 or "").strip()
    vid = (visitor_id or "").strip()
    if not sid.isdigit() or len(sid) != 17 or len(vid) < 16 or len(vid) > 64:
        return (0, 0)
    try:
        conn = _get_conn()
    except OSError:
        return (0, 0)
    try:
        now = int(time.time())
        unique_delta = 0
        with _lock:
            with conn:
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO profile_view_visitors (steamid64, visitor_id, first_seen)
                    VALUES (?, ?, ?)
                    """,
                    (sid, vid, now),
                )
                if cur.rowcount > 0:
                    unique_delta = 1
                conn.execute(
                    """
                    INSERT INTO profile_view_totals (steamid64, total_views, unique_visitors)
                    VALUES (?, 1, ?)
                    ON CONFLICT(steamid64) DO UPDATE SET
                      total_views = total_views + 1,
                      unique_visitors = unique_visitors + ?
                    """,
                    (sid, unique_delta, unique_delta),
                )
            row = conn.execute(
                "SELECT total_views, unique_visitors FROM profile_view_totals WHERE steamid64 = ?",
                (sid,),
            ).fetchone()
        if not row:
            return (0, 0)
        return (int(row[0] or 0), int(row[1] or 0))
    except Exception:
        return (0, 0)
