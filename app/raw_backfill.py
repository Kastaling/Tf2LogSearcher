"""
Backfill raw_events.db from existing raw log zip files on disk.
Processes all log_{id}.log.zip files in RAW_LOGS_DIR.

Usage: python -m app.raw_backfill [--raw-logs-dir ...] [--db-path ...] [--batch-size 200]
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from pathlib import Path

from app.config import RAW_EVENTS_DB_PATH, RAW_LOGS_DIR
from app.raw_zip_io import extract_log_content_from_zip
from app.raw_db import connect_raw_db, init_raw_db, replace_raw_events_for_log
from app.raw_log_parser import parse_raw_log

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

_RAW_ZIP_NAME = re.compile(r"^log_(\d+)\.log\.zip$")


def _iter_raw_zip_ids(raw_logs_dir: Path) -> list[int]:
    out: list[int] = []
    if not raw_logs_dir.is_dir():
        return out
    for p in raw_logs_dir.iterdir():
        if not p.is_file():
            continue
        m = _RAW_ZIP_NAME.match(p.name)
        if m:
            out.append(int(m.group(1)))
    out.sort()
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill raw_events.db from log_*.log.zip files.")
    ap.add_argument("--raw-logs-dir", type=Path, default=RAW_LOGS_DIR, help="Directory with log_<id>.log.zip files")
    ap.add_argument("--db-path", type=Path, default=RAW_EVENTS_DB_PATH, help="SQLite DB path (raw_events.db)")
    ap.add_argument("--batch-size", type=int, default=200, help="Commit every N logs (default 200)")
    args = ap.parse_args()

    raw_dir: Path = args.raw_logs_dir
    db_path: Path = args.db_path
    batch_size = max(1, int(args.batch_size))

    ids = _iter_raw_zip_ids(raw_dir)
    if not ids:
        logger.info("No log_*.log.zip files under %s", raw_dir)
        return

    conn = connect_raw_db(db_path)
    try:
        init_raw_db(conn)
        total = len(ids)
        agg = {"kills": 0, "ubers": 0, "charge_ends": 0, "captures": 0, "spawns": 0}
        t0 = time.perf_counter()
        in_batch = 0

        conn.execute("BEGIN")
        for i, log_id in enumerate(ids, start=1):
            zip_path = raw_dir / f"log_{log_id}.log.zip"
            try:
                zip_bytes = zip_path.read_bytes()
            except OSError as e:
                logger.warning("Skip log %s: %s", log_id, e)
                continue

            content = extract_log_content_from_zip(zip_bytes)
            if content is None:
                logger.warning("Skip log %s: could not read zip", log_id)
                continue
            try:
                parsed = parse_raw_log(log_id, content)
                conn.execute("SAVEPOINT raw_backfill_log")
                try:
                    counts = replace_raw_events_for_log(conn, log_id, parsed)
                except Exception:
                    conn.execute("ROLLBACK TO SAVEPOINT raw_backfill_log")
                    raise
                conn.execute("RELEASE SAVEPOINT raw_backfill_log")
                for k in agg:
                    if k in counts:
                        agg[k] += counts[k]
            except Exception as e:
                logger.warning("Failed log %s: %s", log_id, e)
                continue

            in_batch += 1
            if in_batch >= batch_size:
                conn.commit()
                logger.info(
                    "Progress %s/%s | batch totals: kills=%s ubers=%s charge_ends=%s caps=%s spawns=%s | elapsed=%.1fs",
                    i,
                    total,
                    agg["kills"],
                    agg["ubers"],
                    agg["charge_ends"],
                    agg["captures"],
                    agg["spawns"],
                    time.perf_counter() - t0,
                )
                agg = {"kills": 0, "ubers": 0, "charge_ends": 0, "captures": 0, "spawns": 0}
                in_batch = 0
                conn.execute("BEGIN")

        conn.commit()
        logger.info("Done: processed %s zip file(s) in %.1fs", total, time.perf_counter() - t0)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
