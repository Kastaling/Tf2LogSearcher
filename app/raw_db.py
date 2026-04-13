"""SQLite storage for position and event data parsed from raw TF2 server logs."""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any

def connect_raw_db(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=120.0)
    conn.execute("PRAGMA busy_timeout=120000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    """Add column if missing (idempotent migrations)."""
    row = conn.execute(f"PRAGMA table_info({table})").fetchall()
    names = {str(r[1]) for r in row} if row else set()
    if column not in names:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def init_raw_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS raw_logs (
          log_id        INTEGER PRIMARY KEY,
          imported_at   INTEGER NOT NULL,
          kill_count    INTEGER NOT NULL DEFAULT 0,
          uber_count    INTEGER NOT NULL DEFAULT 0,
          capture_count INTEGER NOT NULL DEFAULT 0,
          spawn_count   INTEGER NOT NULL DEFAULT 0,
          charge_end_count INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS kill_events (
          id                    INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id                INTEGER NOT NULL REFERENCES raw_logs(log_id) ON DELETE CASCADE,
          tick                  INTEGER,
          round_tick            INTEGER,
          attacker_steamid64    TEXT,
          attacker_x            INTEGER,
          attacker_y            INTEGER,
          attacker_z            INTEGER,
          victim_steamid64      TEXT,
          victim_x              INTEGER,
          victim_y              INTEGER,
          victim_z              INTEGER,
          assister_steamid64    TEXT,
          assister_x            INTEGER,
          assister_y            INTEGER,
          assister_z            INTEGER,
          weapon                TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_ke_log_id ON kill_events(log_id);
        CREATE INDEX IF NOT EXISTS idx_ke_attacker ON kill_events(attacker_steamid64);
        CREATE INDEX IF NOT EXISTS idx_ke_victim ON kill_events(victim_steamid64);
        CREATE INDEX IF NOT EXISTS idx_ke_weapon ON kill_events(weapon);
        CREATE INDEX IF NOT EXISTS idx_ke_attacker_log ON kill_events(attacker_steamid64, log_id);
        CREATE INDEX IF NOT EXISTS idx_ke_victim_log ON kill_events(victim_steamid64, log_id);

        CREATE TABLE IF NOT EXISTS uber_events (
          id                INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id            INTEGER NOT NULL REFERENCES raw_logs(log_id) ON DELETE CASCADE,
          tick              INTEGER,
          round_tick        INTEGER,
          medic_steamid64   TEXT,
          pos_x             INTEGER,
          pos_y             INTEGER,
          pos_z             INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_ue_log_id ON uber_events(log_id);
        CREATE INDEX IF NOT EXISTS idx_ue_medic ON uber_events(medic_steamid64);

        CREATE TABLE IF NOT EXISTS charge_end_events (
          id                INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id            INTEGER NOT NULL REFERENCES raw_logs(log_id) ON DELETE CASCADE,
          tick              INTEGER,
          round_tick        INTEGER,
          medic_steamid64   TEXT,
          duration_sec      REAL
        );
        CREATE INDEX IF NOT EXISTS idx_chee_log_id ON charge_end_events(log_id);
        CREATE INDEX IF NOT EXISTS idx_chee_medic ON charge_end_events(medic_steamid64);

        CREATE TABLE IF NOT EXISTS capture_events (
          id            INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id        INTEGER NOT NULL REFERENCES raw_logs(log_id) ON DELETE CASCADE,
          tick          INTEGER,
          round_tick    INTEGER,
          steamid64     TEXT,
          cp_index      INTEGER,
          cp_name       TEXT,
          pos_x         INTEGER,
          pos_y         INTEGER,
          pos_z         INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_cap_log_id ON capture_events(log_id);
        CREATE INDEX IF NOT EXISTS idx_cap_steamid64 ON capture_events(steamid64);

        CREATE TABLE IF NOT EXISTS round_events (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id      INTEGER NOT NULL REFERENCES raw_logs(log_id) ON DELETE CASCADE,
          tick        INTEGER,
          event_type  TEXT NOT NULL,
          winner_team TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_re_log_id ON round_events(log_id);

        CREATE TABLE IF NOT EXISTS spawn_events (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id      INTEGER NOT NULL REFERENCES raw_logs(log_id) ON DELETE CASCADE,
          tick        INTEGER,
          round_tick  INTEGER,
          steamid64   TEXT,
          class_name  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_se_log_id ON spawn_events(log_id);
        CREATE INDEX IF NOT EXISTS idx_se_steamid64 ON spawn_events(steamid64);
        """
    )
    # Older DBs created before charge_end_count / charge_end_events
    _ensure_column(conn, "raw_logs", "charge_end_count", "INTEGER NOT NULL DEFAULT 0")
    conn.commit()


def replace_raw_events_for_log(
    conn: sqlite3.Connection,
    log_id: int,
    parsed: dict[str, list[dict[str, Any]]],
    *,
    imported_at: int | None = None,
) -> dict[str, int]:
    """
    Replace all raw event rows for one log atomically. Caller controls transaction.
    Returns dict of counts: {'kills', 'ubers', 'charge_ends', 'captures', 'spawns'}.
    """
    imp = int(imported_at if imported_at is not None else time.time())
    kills = parsed.get("kill_events") or []
    ubers = parsed.get("uber_events") or []
    charge_ends = parsed.get("charge_end_events") or []
    caps = parsed.get("capture_events") or []
    rounds = parsed.get("round_events") or []
    spawns = parsed.get("spawn_events") or []

    conn.execute("DELETE FROM raw_logs WHERE log_id = ?", (log_id,))
    conn.execute(
        """
        INSERT INTO raw_logs (
          log_id, imported_at, kill_count, uber_count, capture_count, spawn_count, charge_end_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (log_id, imp, len(kills), len(ubers), len(caps), len(spawns), len(charge_ends)),
    )

    def _ke_row(d: dict[str, Any]) -> tuple[Any, ...]:
        return (
            log_id,
            d.get("tick"),
            d.get("round_tick"),
            d.get("attacker_steamid64"),
            d.get("attacker_x"),
            d.get("attacker_y"),
            d.get("attacker_z"),
            d.get("victim_steamid64"),
            d.get("victim_x"),
            d.get("victim_y"),
            d.get("victim_z"),
            d.get("assister_steamid64"),
            d.get("assister_x"),
            d.get("assister_y"),
            d.get("assister_z"),
            d.get("weapon"),
        )

    if kills:
        conn.executemany(
            """
            INSERT INTO kill_events (
              log_id, tick, round_tick,
              attacker_steamid64, attacker_x, attacker_y, attacker_z,
              victim_steamid64, victim_x, victim_y, victim_z,
              assister_steamid64, assister_x, assister_y, assister_z,
              weapon
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (_ke_row(d) for d in kills),
        )

    def _ue_row(d: dict[str, Any]) -> tuple[Any, ...]:
        return (
            log_id,
            d.get("tick"),
            d.get("round_tick"),
            d.get("medic_steamid64"),
            d.get("pos_x"),
            d.get("pos_y"),
            d.get("pos_z"),
        )

    if ubers:
        conn.executemany(
            """
            INSERT INTO uber_events (log_id, tick, round_tick, medic_steamid64, pos_x, pos_y, pos_z)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (_ue_row(d) for d in ubers),
        )

    def _che_row(d: dict[str, Any]) -> tuple[Any, ...]:
        return (
            log_id,
            d.get("tick"),
            d.get("round_tick"),
            d.get("medic_steamid64"),
            d.get("duration_sec"),
        )

    if charge_ends:
        conn.executemany(
            """
            INSERT INTO charge_end_events (log_id, tick, round_tick, medic_steamid64, duration_sec)
            VALUES (?, ?, ?, ?, ?)
            """,
            (_che_row(d) for d in charge_ends),
        )

    def _ce_row(d: dict[str, Any]) -> tuple[Any, ...]:
        return (
            log_id,
            d.get("tick"),
            d.get("round_tick"),
            d.get("steamid64"),
            d.get("cp_index"),
            d.get("cp_name"),
            d.get("pos_x"),
            d.get("pos_y"),
            d.get("pos_z"),
        )

    if caps:
        conn.executemany(
            """
            INSERT INTO capture_events (
              log_id, tick, round_tick, steamid64, cp_index, cp_name, pos_x, pos_y, pos_z
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (_ce_row(d) for d in caps),
        )

    if rounds:
        conn.executemany(
            """
            INSERT INTO round_events (log_id, tick, event_type, winner_team)
            VALUES (?, ?, ?, ?)
            """,
            (
                (log_id, d.get("tick"), d.get("event_type"), d.get("winner_team"))
                for d in rounds
            ),
        )

    def _se_row(d: dict[str, Any]) -> tuple[Any, ...]:
        return (
            log_id,
            d.get("tick"),
            d.get("round_tick"),
            d.get("steamid64"),
            d.get("class_name"),
        )

    if spawns:
        conn.executemany(
            """
            INSERT INTO spawn_events (log_id, tick, round_tick, steamid64, class_name)
            VALUES (?, ?, ?, ?, ?)
            """,
            (_se_row(d) for d in spawns),
        )

    return {
        "kills": len(kills),
        "ubers": len(ubers),
        "charge_ends": len(charge_ends),
        "captures": len(caps),
        "spawns": len(spawns),
    }


def count_raw_library_rows(db_path: str | Path) -> tuple[int | None, int | None]:
    """
    Return (COUNT(raw_logs), SUM(kill_count)) for progress UI.
    (None, None) if the DB is missing or unreadable.
    """
    path = Path(db_path)
    if not path.is_file():
        return (None, None)
    try:
        conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10.0)
        try:
            conn.execute("PRAGMA busy_timeout=10000")
            conn.execute("SELECT 1 FROM raw_logs LIMIT 1").fetchone()
            cnt_row = conn.execute("SELECT COUNT(*) FROM raw_logs").fetchone()
            n = int(cnt_row[0] or 0) if cnt_row else 0
            cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(raw_logs)").fetchall()}
            if "kill_count" in cols:
                s = conn.execute("SELECT COALESCE(SUM(kill_count), 0) FROM raw_logs").fetchone()
                kill_sum = int(s[0] or 0) if s else 0
            else:
                kill_sum = 0
            return (n, kill_sum)
        except Exception:
            return (None, None)
        finally:
            conn.close()
    except Exception:
        return (None, None)


def raw_db_fingerprint(db_path: str | Path) -> frozenset[int]:
    """(log_count, max_log_id, max_imported_at) fingerprint. Same pattern as stats_db_fingerprint."""
    path = Path(db_path)
    if not path.is_file():
        return frozenset()
    try:
        conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10.0)
        try:
            conn.execute("PRAGMA busy_timeout=10000")
            row = conn.execute(
                "SELECT COUNT(*), COALESCE(MAX(log_id), 0), COALESCE(MAX(imported_at), 0) FROM raw_logs"
            ).fetchone()
        finally:
            conn.close()
    except Exception:
        return frozenset()
    count = int(row[0] or 0) if row else 0
    max_id = int(row[1] or 0) if row else 0
    max_imp = int(row[2] or 0) if row else 0
    return frozenset((count, max_id, max_imp))
