"""Resumable maintenance job for the latest player-name lookup table.

Stop web and downloader before running against the production stats database.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from app.config import STATS_DB_PATH
from app.stats_db import connect_stats_db, init_stats_db

logger = logging.getLogger(__name__)

_UPSERT = """
    INSERT INTO player_latest_names (steamid64, alias, log_id, date_ts)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(steamid64) DO UPDATE SET
      alias = excluded.alias, log_id = excluded.log_id, date_ts = excluded.date_ts
    WHERE COALESCE(excluded.date_ts, 0) > COALESCE(player_latest_names.date_ts, 0)
       OR (COALESCE(excluded.date_ts, 0) = COALESCE(player_latest_names.date_ts, 0)
           AND excluded.log_id >= player_latest_names.log_id)
"""


def run(db_path: Path, *, batch_size: int = 10000, rebuild: bool = False) -> int:
    """Return processed source-row count; commits a resume cursor after each batch."""
    if batch_size < 1 or batch_size > 100000:
        raise ValueError("batch_size must be between 1 and 100000")
    conn = connect_stats_db(db_path)
    try:
        init_stats_db(conn)
        ready = conn.execute(
            "SELECT row_count FROM stats_cache_generation WHERE name = 'latest_names_ready'"
        ).fetchone()
        if ready and int(ready[0]) and not rebuild:
            logger.info("Latest-name table is already ready; use --rebuild to start over.")
            return 0
        progress = conn.execute(
            "SELECT row_count FROM stats_cache_generation WHERE name = 'latest_names_progress'"
        ).fetchone()
        if progress is None or rebuild:
            with conn:
                conn.execute("DELETE FROM player_latest_names")
                conn.execute(
                    "INSERT OR REPLACE INTO stats_cache_generation "
                    "(name, generation, row_count) VALUES ('latest_names_progress', 0, 0)"
                )
                conn.execute(
                    "UPDATE stats_cache_generation SET row_count = 0 "
                    "WHERE name = 'latest_names_ready'"
                )
            cursor = 0
        else:
            cursor = int(progress[0])

        processed = 0
        while True:
            rows = conn.execute(
                """
                SELECT pn.rowid, pn.steamid64, pn.alias, pn.log_id, pn.date_ts,
                       l.log_id IS NOT NULL AS indexed
                FROM player_names pn
                LEFT JOIN logs l ON l.log_id = pn.log_id
                WHERE pn.rowid > ?
                ORDER BY pn.rowid
                LIMIT ?
                """,
                (cursor, batch_size),
            ).fetchall()
            if not rows:
                break
            with conn:
                conn.executemany(
                    _UPSERT,
                    (
                        (sid, alias, log_id, date_ts)
                        for _, sid, alias, log_id, date_ts, indexed in rows
                        if indexed and str(alias or "").strip()
                    ),
                )
                cursor = int(rows[-1][0])
                conn.execute(
                    "UPDATE stats_cache_generation SET row_count = ? "
                    "WHERE name = 'latest_names_progress'",
                    (cursor,),
                )
            processed += len(rows)
            logger.info("Latest names: %s source rows processed; cursor %s", processed, cursor)

        with conn:
            conn.execute(
                "UPDATE stats_cache_generation SET row_count = 1 WHERE name = 'latest_names_ready'"
            )
            conn.execute(
                "UPDATE stats_cache_generation SET generation = generation + 1 WHERE name = 'names'"
            )
        logger.info("Latest-name lookup is ready (%s rows processed this run).", processed)
        return processed
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=STATS_DB_PATH)
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(args.db_path, batch_size=args.batch_size, rebuild=args.rebuild)


if __name__ == "__main__":
    main()
