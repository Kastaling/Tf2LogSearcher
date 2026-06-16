"""SQLite storage for position and event data parsed from raw TF2 server logs."""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any

# SQLite INTEGER is signed 64-bit.
_SQLITE_INT_MAX = (1 << 63) - 1
_SQLITE_INT_MIN = -(1 << 63)


def clamp_sqlite_int(value: int | None) -> int | None:
    """Return value if it fits SQLite INTEGER; else None (drop out-of-range coords)."""
    if value is None:
        return None
    if value > _SQLITE_INT_MAX or value < _SQLITE_INT_MIN:
        return None
    return value


def is_sqlite_corrupt_error(exc: BaseException) -> bool:
    """True when SQLite reports database corruption (connection stays unusable until reopen)."""
    if not isinstance(exc, sqlite3.Error):
        return False
    msg = str(exc).lower()
    return "malformed" in msg or "not a database" in msg or (
        "corrupt" in msg and "unicode" not in msg
    )


def checkpoint_raw_db(db_path: str | Path, *, truncate: bool = True) -> None:
    """Flush WAL; TRUNCATE helps long write sessions and after reconnect."""
    conn = connect_raw_db(db_path)
    try:
        mode = "TRUNCATE" if truncate else "PASSIVE"
        conn.execute(f"PRAGMA wal_checkpoint({mode})")
    finally:
        conn.close()


def quick_raw_db_ok(db_path: str | Path) -> bool:
    """Fast sanity check (not a full integrity_check on huge DBs)."""
    path = Path(db_path)
    if not path.is_file() or path.stat().st_size < 100:
        return False
    try:
        conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=30.0)
        try:
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("SELECT 1 FROM raw_logs LIMIT 1")
            return True
        finally:
            conn.close()
    except sqlite3.Error:
        return False


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

        CREATE TABLE IF NOT EXISTS charge_ready_events (
          id                INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id            INTEGER NOT NULL REFERENCES raw_logs(log_id) ON DELETE CASCADE,
          tick              INTEGER,
          round_tick        INTEGER,
          medic_steamid64   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_cre_log_id ON charge_ready_events(log_id);
        CREATE INDEX IF NOT EXISTS idx_cre_medic ON charge_ready_events(medic_steamid64);

        CREATE TABLE IF NOT EXISTS lost_advantage_events (
          id                INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id            INTEGER NOT NULL REFERENCES raw_logs(log_id) ON DELETE CASCADE,
          tick              INTEGER,
          round_tick        INTEGER,
          medic_steamid64   TEXT,
          advantage_sec     REAL
        );
        CREATE INDEX IF NOT EXISTS idx_lae_log_id ON lost_advantage_events(log_id);
        CREATE INDEX IF NOT EXISTS idx_lae_medic ON lost_advantage_events(medic_steamid64);

        CREATE TABLE IF NOT EXISTS medic_death_events (
          id                  INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id              INTEGER NOT NULL REFERENCES raw_logs(log_id) ON DELETE CASCADE,
          tick                INTEGER,
          round_tick          INTEGER,
          killer_steamid64    TEXT,
          medic_steamid64     TEXT,
          healing             INTEGER,
          had_uber            INTEGER NOT NULL DEFAULT 0,
          uber_pct            INTEGER,
          pos_x               INTEGER,
          pos_y               INTEGER,
          pos_z               INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_mde_log_id ON medic_death_events(log_id);
        CREATE INDEX IF NOT EXISTS idx_mde_medic ON medic_death_events(medic_steamid64);

        CREATE TABLE IF NOT EXISTS empty_uber_events (
          id                INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id            INTEGER NOT NULL REFERENCES raw_logs(log_id) ON DELETE CASCADE,
          tick              INTEGER,
          round_tick        INTEGER,
          medic_steamid64   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_eue_log_id ON empty_uber_events(log_id);
        CREATE INDEX IF NOT EXISTS idx_eue_medic ON empty_uber_events(medic_steamid64);

        CREATE TABLE IF NOT EXISTS capture_blocked_events (
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
        CREATE INDEX IF NOT EXISTS idx_cbe_log_id ON capture_blocked_events(log_id);
        CREATE INDEX IF NOT EXISTS idx_cbe_steamid64 ON capture_blocked_events(steamid64);

        CREATE TABLE IF NOT EXISTS passtime_events (
          id                  INTEGER PRIMARY KEY AUTOINCREMENT,
          log_id              INTEGER NOT NULL REFERENCES raw_logs(log_id) ON DELETE CASCADE,
          tick                INTEGER,
          round_tick          INTEGER,
          event_type          TEXT NOT NULL,
          steamid64           TEXT,
          other_steamid64     TEXT,
          points              INTEGER,
          first_contact       INTEGER,
          interception        INTEGER,
          save                INTEGER,
          handoff             INTEGER,
          dist                REAL,
          duration_sec        REAL,
          speed               INTEGER,
          panacea             INTEGER,
          win_strat           INTEGER,
          deathbomb           INTEGER,
          steal_defense       INTEGER,
          pos_x               INTEGER,
          pos_y               INTEGER,
          pos_z               INTEGER,
          thrower_pos_x       INTEGER,
          thrower_pos_y       INTEGER,
          thrower_pos_z       INTEGER,
          catcher_pos_x       INTEGER,
          catcher_pos_y       INTEGER,
          catcher_pos_z       INTEGER,
          thief_pos_x         INTEGER,
          thief_pos_y         INTEGER,
          thief_pos_z         INTEGER,
          victim_pos_x        INTEGER,
          victim_pos_y        INTEGER,
          victim_pos_z        INTEGER,
          ball_pos_x          INTEGER,
          ball_pos_y          INTEGER,
          ball_pos_z          INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_pte_log_id ON passtime_events(log_id);
        CREATE INDEX IF NOT EXISTS idx_pte_type ON passtime_events(event_type);

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
    _ensure_column(conn, "raw_logs", "charge_ready_count", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "raw_logs", "lost_advantage_count", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "raw_logs", "medic_death_count", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "raw_logs", "empty_uber_count", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "raw_logs", "capture_blocked_count", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "raw_logs", "passtime_count", "INTEGER NOT NULL DEFAULT 0")
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
    Returns dict of counts: {'kills', 'ubers', 'charge_ends', 'charge_readies',
    'lost_advantages', 'medic_deaths', 'empty_ubers', 'capture_blocked',
    'passtime', 'captures', 'spawns'}.
    """
    imp = int(imported_at if imported_at is not None else time.time())
    kills = parsed.get("kill_events") or []
    ubers = parsed.get("uber_events") or []
    charge_ends = parsed.get("charge_end_events") or []
    charge_readies = parsed.get("charge_ready_events") or []
    lost_advantages = parsed.get("lost_advantage_events") or []
    medic_deaths = parsed.get("medic_death_events") or []
    empty_ubers = parsed.get("empty_uber_events") or []
    capture_blocked = parsed.get("capture_blocked_events") or []
    passtime = parsed.get("passtime_events") or []
    caps = parsed.get("capture_events") or []
    rounds = parsed.get("round_events") or []
    spawns = parsed.get("spawn_events") or []

    conn.execute("DELETE FROM raw_logs WHERE log_id = ?", (log_id,))
    conn.execute(
        """
        INSERT INTO raw_logs (
          log_id, imported_at, kill_count, uber_count, capture_count, spawn_count,
          charge_end_count, charge_ready_count, lost_advantage_count,
          medic_death_count, empty_uber_count, capture_blocked_count, passtime_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            log_id,
            imp,
            len(kills),
            len(ubers),
            len(caps),
            len(spawns),
            len(charge_ends),
            len(charge_readies),
            len(lost_advantages),
            len(medic_deaths),
            len(empty_ubers),
            len(capture_blocked),
            len(passtime),
        ),
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

    def _cr_row(d: dict[str, Any]) -> tuple[Any, ...]:
        return (
            log_id,
            d.get("tick"),
            d.get("round_tick"),
            d.get("medic_steamid64"),
        )

    if charge_readies:
        conn.executemany(
            """
            INSERT INTO charge_ready_events (log_id, tick, round_tick, medic_steamid64)
            VALUES (?, ?, ?, ?)
            """,
            (_cr_row(d) for d in charge_readies),
        )

    def _la_row(d: dict[str, Any]) -> tuple[Any, ...]:
        return (
            log_id,
            d.get("tick"),
            d.get("round_tick"),
            d.get("medic_steamid64"),
            d.get("advantage_sec"),
        )

    if lost_advantages:
        conn.executemany(
            """
            INSERT INTO lost_advantage_events (
              log_id, tick, round_tick, medic_steamid64, advantage_sec
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (_la_row(d) for d in lost_advantages),
        )

    def _mde_row(d: dict[str, Any]) -> tuple[Any, ...]:
        return (
            log_id,
            d.get("tick"),
            d.get("round_tick"),
            d.get("killer_steamid64"),
            d.get("medic_steamid64"),
            d.get("healing"),
            int(d.get("had_uber") or 0),
            d.get("uber_pct"),
            d.get("pos_x"),
            d.get("pos_y"),
            d.get("pos_z"),
        )

    if medic_deaths:
        conn.executemany(
            """
            INSERT INTO medic_death_events (
              log_id, tick, round_tick, killer_steamid64, medic_steamid64,
              healing, had_uber, uber_pct, pos_x, pos_y, pos_z
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (_mde_row(d) for d in medic_deaths),
        )

    def _eu_row(d: dict[str, Any]) -> tuple[Any, ...]:
        return (
            log_id,
            d.get("tick"),
            d.get("round_tick"),
            d.get("medic_steamid64"),
        )

    if empty_ubers:
        conn.executemany(
            """
            INSERT INTO empty_uber_events (log_id, tick, round_tick, medic_steamid64)
            VALUES (?, ?, ?, ?)
            """,
            (_eu_row(d) for d in empty_ubers),
        )

    def _cb_row(d: dict[str, Any]) -> tuple[Any, ...]:
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

    if capture_blocked:
        conn.executemany(
            """
            INSERT INTO capture_blocked_events (
              log_id, tick, round_tick, steamid64, cp_index, cp_name, pos_x, pos_y, pos_z
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (_cb_row(d) for d in capture_blocked),
        )

    def _pt_row(d: dict[str, Any]) -> tuple[Any, ...]:
        return (
            log_id,
            d.get("tick"),
            d.get("round_tick"),
            d.get("event_type"),
            d.get("steamid64"),
            d.get("other_steamid64"),
            d.get("points"),
            d.get("first_contact"),
            d.get("interception"),
            d.get("save"),
            d.get("handoff"),
            d.get("dist"),
            d.get("duration_sec"),
            d.get("speed"),
            d.get("panacea"),
            d.get("win_strat"),
            d.get("deathbomb"),
            d.get("steal_defense"),
            d.get("pos_x"),
            d.get("pos_y"),
            d.get("pos_z"),
            d.get("thrower_pos_x"),
            d.get("thrower_pos_y"),
            d.get("thrower_pos_z"),
            d.get("catcher_pos_x"),
            d.get("catcher_pos_y"),
            d.get("catcher_pos_z"),
            d.get("thief_pos_x"),
            d.get("thief_pos_y"),
            d.get("thief_pos_z"),
            d.get("victim_pos_x"),
            d.get("victim_pos_y"),
            d.get("victim_pos_z"),
            d.get("ball_pos_x"),
            d.get("ball_pos_y"),
            d.get("ball_pos_z"),
        )

    if passtime:
        conn.executemany(
            """
            INSERT INTO passtime_events (
              log_id, tick, round_tick, event_type, steamid64, other_steamid64,
              points, first_contact, interception, save, handoff, dist, duration_sec,
              speed, panacea, win_strat, deathbomb, steal_defense,
              pos_x, pos_y, pos_z,
              thrower_pos_x, thrower_pos_y, thrower_pos_z,
              catcher_pos_x, catcher_pos_y, catcher_pos_z,
              thief_pos_x, thief_pos_y, thief_pos_z,
              victim_pos_x, victim_pos_y, victim_pos_z,
              ball_pos_x, ball_pos_y, ball_pos_z
            ) VALUES (
              ?, ?, ?, ?, ?, ?,
              ?, ?, ?, ?, ?, ?, ?,
              ?, ?, ?, ?, ?,
              ?, ?, ?,
              ?, ?, ?,
              ?, ?, ?,
              ?, ?, ?,
              ?, ?, ?,
              ?, ?, ?
            )
            """,
            (_pt_row(d) for d in passtime),
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
        "charge_readies": len(charge_readies),
        "lost_advantages": len(lost_advantages),
        "medic_deaths": len(medic_deaths),
        "empty_ubers": len(empty_ubers),
        "capture_blocked": len(capture_blocked),
        "passtime": len(passtime),
        "captures": len(caps),
        "spawns": len(spawns),
    }


def count_raw_library_rows(db_path: str | Path) -> tuple[int | None, int | None]:
    """
    Return (``raw_logs`` row count, SUM(kill_count)) for progress UI.

    When ``kill_count`` exists (normal schema), both values come from one aggregate query over
    ``raw_logs`` — the same work previously done only for ``SUM``, plus ``COUNT(*)``.

    (None, None) if the DB is missing or unreadable.
    """
    path = Path(db_path)
    if not path.is_file():
        return (None, None)
    try:
        conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10.0)
        try:
            conn.execute("PRAGMA busy_timeout=10000")
            cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(raw_logs)").fetchall()}
            if "kill_count" in cols:
                row = conn.execute(
                    "SELECT COUNT(*), COALESCE(SUM(kill_count), 0) FROM raw_logs"
                ).fetchone()
                if not row:
                    return (0, 0)
                n = int(row[0] or 0)
                kill_sum = int(row[1] or 0)
            else:
                cnt_row = conn.execute("SELECT COUNT(*) FROM raw_logs").fetchone()
                n = int(cnt_row[0] or 0) if cnt_row else 0
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
