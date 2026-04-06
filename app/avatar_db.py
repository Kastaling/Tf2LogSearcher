"""SQLite cache for Steam avatar URLs (separate DB from chat)."""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

_AVATAR_CACHE_TTL_SEC = 7 * 24 * 60 * 60


def connect_avatar_db(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_avatar_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS avatars (
          steamid64 TEXT PRIMARY KEY,
          avatar_url TEXT NOT NULL,
          fetched_at INTEGER NOT NULL
        );
        """
    )
    conn.commit()


def get_cached_avatar(conn: sqlite3.Connection, steamid64: str) -> str | None:
    """Return cached avatar_url if present and fetched within 7 days, else None."""
    row = conn.execute(
        "SELECT avatar_url, fetched_at FROM avatars WHERE steamid64 = ?",
        (steamid64,),
    ).fetchone()
    if not row:
        return None
    url, fetched_at = row[0], row[1]
    try:
        age = time.time() - int(fetched_at)
    except (TypeError, ValueError):
        return None
    if age > _AVATAR_CACHE_TTL_SEC:
        return None
    return str(url) if url else None


def set_cached_avatar(conn: sqlite3.Connection, steamid64: str, avatar_url: str) -> None:
    """Upsert avatar into cache with current timestamp."""
    conn.execute(
        "INSERT OR REPLACE INTO avatars (steamid64, avatar_url, fetched_at) VALUES (?, ?, ?)",
        (steamid64, avatar_url, int(time.time())),
    )
    conn.commit()


def get_cached_avatars_bulk(conn: sqlite3.Connection, steamid64s: list[str]) -> dict[str, str]:
    """
    Return a dict of {steamid64: avatar_url} for all IDs that are cached and within TTL.
    IDs not in cache or past TTL are omitted from the result.
    """
    if not steamid64s:
        return {}
    placeholders = ",".join("?" * len(steamid64s))
    rows = conn.execute(
        f"SELECT steamid64, avatar_url, fetched_at FROM avatars WHERE steamid64 IN ({placeholders})",
        tuple(steamid64s),
    ).fetchall()
    now = time.time()
    out: dict[str, str] = {}
    for steamid64, avatar_url, fetched_at in rows:
        try:
            age = now - int(fetched_at)
        except (TypeError, ValueError):
            continue
        if age > _AVATAR_CACHE_TTL_SEC:
            continue
        if avatar_url:
            out[str(steamid64)] = str(avatar_url)
    return out


def set_cached_avatars_bulk(conn: sqlite3.Connection, avatars: dict[str, str]) -> None:
    """
    Upsert multiple avatar URLs at once in a single transaction.
    """
    if not avatars:
        return
    ts = int(time.time())
    rows = [(sid, url, ts) for sid, url in avatars.items()]
    conn.executemany(
        "INSERT OR REPLACE INTO avatars (steamid64, avatar_url, fetched_at) VALUES (?, ?, ?)",
        rows,
    )
    conn.commit()
