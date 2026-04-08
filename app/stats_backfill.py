"""One-time backfill: import stats from existing local log JSON files into stats SQLite DB."""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from app.config import LOGS_DIR, STATS_DB_PATH
from app.stats_db import connect_stats_db, init_stats_db, replace_stats_for_log

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)


def _iter_log_files(logs_dir: Path) -> list[Path]:
    out: list[Path] = []
    for p in logs_dir.iterdir():
        if not p.is_file() or p.suffix != ".json":
            continue
        if not p.stem.isdigit():
            continue
        out.append(p)
    out.sort(key=lambda p: int(p.stem))
    return out


def run_backfill(logs_dir: Path, db_path: Path, batch_size: int) -> None:
    files = _iter_log_files(logs_dir)
    logger.info("Found %s local log file(s) to scan in %s", len(files), logs_dir)
    conn = connect_stats_db(db_path)
    try:
        init_stats_db(conn)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        raise
    logger.info("Writing stats rows into %s", db_path)

    start = time.time()
    processed = 0
    player_rows_total = 0
    parse_errors = 0

    conn.execute("BEGIN")
    try:
        for p in files:
            processed += 1
            try:
                log_id = int(p.stem)
                logtext = json.loads(p.read_text(encoding="utf-8", errors="replace"))
                player_rows_total += replace_stats_for_log(conn, log_id, logtext)
            except (OSError, ValueError, TypeError) as e:
                parse_errors += 1
                logger.warning("Skipping %s due to parse/read error: %s", p.name, e)
                continue

            if processed % batch_size == 0:
                conn.commit()
                elapsed = max(0.001, time.time() - start)
                logger.info(
                    "Progress: %s/%s logs processed, %s player rows inserted (%.1f logs/s)",
                    processed,
                    len(files),
                    player_rows_total,
                    processed / elapsed,
                )
                conn.execute("BEGIN")
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()

    elapsed = max(0.001, time.time() - start)
    logger.info(
        "Backfill complete: processed=%s player_rows=%s parse_errors=%s elapsed=%.1fs (%.1f logs/s)",
        processed,
        player_rows_total,
        parse_errors,
        elapsed,
        processed / elapsed,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill stats SQLite DB from existing local log JSON files."
    )
    parser.add_argument("--logs-dir", default=str(LOGS_DIR), help="Directory containing local <id>.json log files")
    parser.add_argument("--db-path", default=str(STATS_DB_PATH), help="SQLite DB file path for stats data")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="How many logs per transaction commit (higher is faster, lower uses less rollback work on failure)",
    )
    args = parser.parse_args()

    logs_dir = Path(args.logs_dir)
    db_path = Path(args.db_path)
    batch_size = max(1, int(args.batch_size))
    if not logs_dir.exists() or not logs_dir.is_dir():
        raise SystemExit(f"Invalid --logs-dir: {logs_dir}")

    run_backfill(logs_dir, db_path, batch_size)


if __name__ == "__main__":
    main()
