"""
Backfill raw_events.db from existing raw log zip files on disk.
Processes all log_{id}.log.zip files in RAW_LOGS_DIR.

Usage: python -m app.raw_backfill [--raw-logs-dir ...] [--db-path ...] [--batch-size 200]
       [--min-log-id ID] [--max-log-id ID] [--skip-files N]
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

_AGG_KEYS = (
    "kills",
    "ubers",
    "charge_ends",
    "charge_readies",
    "lost_advantages",
    "medic_deaths",
    "empty_ubers",
    "capture_blocked",
    "passtime",
    "captures",
    "spawns",
)


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


def _filter_log_ids(
    ids: list[int],
    *,
    min_log_id: int | None,
    max_log_id: int | None,
    skip_files: int,
) -> list[int]:
    out = ids
    if min_log_id is not None:
        out = [i for i in out if i >= min_log_id]
    if max_log_id is not None:
        out = [i for i in out if i <= max_log_id]
    sk = max(0, int(skip_files))
    if sk:
        if sk >= len(out):
            return []
        out = out[sk:]
    return out


def _empty_agg() -> dict[str, int]:
    return {k: 0 for k in _AGG_KEYS}


def run_backfill(
    raw_dir: Path,
    db_path: Path,
    batch_size: int,
    *,
    min_log_id: int | None = None,
    max_log_id: int | None = None,
    skip_files: int = 0,
) -> None:
    ids_all = _iter_raw_zip_ids(raw_dir)
    ids = _filter_log_ids(
        ids_all,
        min_log_id=min_log_id,
        max_log_id=max_log_id,
        skip_files=skip_files,
    )
    logger.info(
        "Found %s raw zip(s) under %s; %s to process "
        "(min_log_id=%s max_log_id=%s skip_files=%s)",
        len(ids_all),
        raw_dir,
        len(ids),
        min_log_id,
        max_log_id,
        skip_files,
    )
    if not ids:
        logger.warning("No raw zips match filters; nothing to do (raw_events.db unchanged).")
        return

    conn = connect_raw_db(db_path)
    try:
        init_raw_db(conn)
        total = len(ids)
        agg = _empty_agg()
        t0 = time.perf_counter()
        in_batch = 0
        processed = 0
        failed = 0

        conn.execute("BEGIN")
        for i, log_id in enumerate(ids, start=1):
            zip_path = raw_dir / f"log_{log_id}.log.zip"
            try:
                zip_bytes = zip_path.read_bytes()
            except OSError as e:
                logger.warning("Skip log %s: %s", log_id, e)
                failed += 1
                continue

            content = extract_log_content_from_zip(zip_bytes)
            if content is None:
                logger.warning("Skip log %s: could not read zip", log_id)
                failed += 1
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
                for k in _AGG_KEYS:
                    if k in counts:
                        agg[k] += counts[k]
                processed += 1
            except Exception as e:
                logger.warning("Failed log %s: %s", log_id, e)
                failed += 1
                continue

            in_batch += 1
            if in_batch >= batch_size:
                conn.commit()
                logger.info(
                    "Progress %s/%s | batch totals: kills=%s ubers=%s charge_ends=%s "
                    "charge_readies=%s lost_advantages=%s medic_deaths=%s empty_ubers=%s "
                    "capture_blocked=%s passtime=%s caps=%s spawns=%s | "
                    "processed=%s failed=%s | elapsed=%.1fs",
                    i,
                    total,
                    agg["kills"],
                    agg["ubers"],
                    agg["charge_ends"],
                    agg["charge_readies"],
                    agg["lost_advantages"],
                    agg["medic_deaths"],
                    agg["empty_ubers"],
                    agg["capture_blocked"],
                    agg["passtime"],
                    agg["captures"],
                    agg["spawns"],
                    processed,
                    failed,
                    time.perf_counter() - t0,
                )
                agg = _empty_agg()
                in_batch = 0
                conn.execute("BEGIN")

        conn.commit()
        elapsed = time.perf_counter() - t0
        logger.info(
            "Done: processed=%s failed=%s zip(s) in %.1fs (%.1f logs/s)",
            processed,
            failed,
            elapsed,
            processed / max(0.001, elapsed),
        )
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Backfill raw_events.db from log_<id>.log.zip files."
    )
    ap.add_argument("--raw-logs-dir", type=Path, default=RAW_LOGS_DIR, help="Directory with log_<id>.log.zip files")
    ap.add_argument("--db-path", type=Path, default=RAW_EVENTS_DB_PATH, help="SQLite DB path (raw_events.db)")
    ap.add_argument("--batch-size", type=int, default=200, help="Commit every N logs (default 200)")
    ap.add_argument(
        "--min-log-id",
        type=int,
        default=None,
        metavar="ID",
        help="Only re-parse zips whose numeric id is >= ID (after sorting)",
    )
    ap.add_argument(
        "--max-log-id",
        type=int,
        default=None,
        metavar="ID",
        help="Only re-parse zips whose numeric id is <= ID (after sorting)",
    )
    ap.add_argument(
        "--skip-files",
        type=int,
        default=0,
        metavar="N",
        help=(
            "Skip the first N zips after sorting and min/max filters — resume by setting N "
            "to the first number from the last successful Progress line (e.g. Progress 2409500/…)"
        ),
    )
    args = ap.parse_args()

    batch_size = max(1, int(args.batch_size))
    if args.skip_files < 0:
        raise SystemExit("--skip-files must be >= 0")
    if args.min_log_id is not None and args.max_log_id is not None:
        if args.min_log_id > args.max_log_id:
            raise SystemExit("--min-log-id must be <= --max-log-id")

    run_backfill(
        args.raw_logs_dir,
        args.db_path,
        batch_size,
        min_log_id=args.min_log_id,
        max_log_id=args.max_log_id,
        skip_files=args.skip_files,
    )


if __name__ == "__main__":
    main()
