"""One-off migration: rebuild ``log_rounds`` for logs already in stats.db from local JSON.

Uses the same ``extract_log_stats`` round logic as the downloader (``duration``/``length``,
first blood from ``events``). Only updates the ``log_rounds`` table.

Run inside the downloader container (same as other backfills)::

    docker-compose stop downloader
    docker-compose run --rm downloader python -m app.fix_log_rounds_from_json
    docker-compose up -d downloader
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path

from app.config import LOGS_DIR, STATS_DB_PATH
from app.stats_db import extract_log_stats

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)


def _replace_rounds_for_log(conn: sqlite3.Connection, log_id: int, logtext: dict) -> int:
    """Delete all ``log_rounds`` for ``log_id`` and insert rows from ``extract_log_stats``."""
    data = extract_log_stats(log_id, logtext)
    rr = data["round_rows"]
    conn.execute("DELETE FROM log_rounds WHERE log_id = ?", (log_id,))
    if not rr:
        return 0
    conn.executemany(
        """
        INSERT INTO log_rounds (log_id, round_idx, duration_secs, winner, first_blood_steamid64, red_kills, blue_kills)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r["log_id"],
                r["round_idx"],
                r["duration_secs"],
                r["winner"],
                r["first_blood_steamid64"],
                r["red_kills"],
                r["blue_kills"],
            )
            for r in rr
        ],
    )
    return len(rr)


def run_fix(
    logs_dir: Path,
    db_path: Path,
    *,
    from_id: int | None,
    to_id: int | None,
    dry_run: bool,
) -> None:
    if not db_path.is_file():
        raise SystemExit(f"Stats database not found: {db_path}")
    if not logs_dir.is_dir():
        raise SystemExit(f"Logs directory not found: {logs_dir}")

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        rows = conn.execute("SELECT log_id FROM logs ORDER BY log_id").fetchall()
    finally:
        conn.close()

    log_ids = [int(r[0]) for r in rows if r and r[0] is not None]
    if from_id is not None:
        log_ids = [lid for lid in log_ids if lid >= from_id]
    if to_id is not None:
        log_ids = [lid for lid in log_ids if lid <= to_id]

    total = len(log_ids)
    logger.info("Processing %s log(s) from %s using JSON from %s", total, db_path, logs_dir)

    updated = 0
    skipped_no_file = 0
    skipped_bad_json = 0
    round_rows_total = 0
    t0 = time.perf_counter()

    wr_conn = None if dry_run else sqlite3.connect(str(db_path))
    if wr_conn is not None:
        wr_conn.execute("PRAGMA foreign_keys=ON")

    try:
        for i, log_id in enumerate(log_ids):
            path = logs_dir / f"{log_id}.json"
            if not path.is_file():
                skipped_no_file += 1
                continue
            try:
                logtext = json.loads(path.read_text(encoding="utf-8", errors="replace"))
            except (OSError, ValueError) as e:
                logger.warning("log_id=%s skip (read/parse error): %s", log_id, e)
                skipped_bad_json += 1
                continue

            if dry_run:
                data = extract_log_stats(log_id, logtext)
                n = len(data["round_rows"])
                round_rows_total += n
            else:
                assert wr_conn is not None
                with wr_conn:
                    n = _replace_rounds_for_log(wr_conn, log_id, logtext)
                round_rows_total += n
            updated += 1

            if (i + 1) % 500 == 0 or (i + 1) == total:
                elapsed = time.perf_counter() - t0
                logger.info(
                    "Progress: %s/%s logs (updated=%s, %.1fs)",
                    i + 1,
                    total,
                    updated,
                    elapsed,
                )
    finally:
        if wr_conn is not None:
            wr_conn.close()

    elapsed = time.perf_counter() - t0
    logger.info(
        "Done: updated=%s skipped_no_file=%s skipped_bad_json=%s round_rows=%s dry_run=%s elapsed=%.1fs",
        updated,
        skipped_no_file,
        skipped_bad_json,
        round_rows_total,
        dry_run,
        elapsed,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rebuild log_rounds from local JSON for every log_id present in stats.db."
    )
    parser.add_argument("--logs-dir", default=str(LOGS_DIR), help="Directory containing local <id>.json log files")
    parser.add_argument("--db-path", default=str(STATS_DB_PATH), help="SQLite stats DB path")
    parser.add_argument("--from-id", type=int, default=None, help="Minimum log_id (inclusive)")
    parser.add_argument("--to-id", type=int, default=None, help="Maximum log_id (inclusive)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse JSON only; do not write to the database",
    )
    args = parser.parse_args()

    logs_dir = Path(args.logs_dir)
    db_path = Path(args.db_path)
    run_fix(
        logs_dir,
        db_path,
        from_id=args.from_id,
        to_id=args.to_id,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
