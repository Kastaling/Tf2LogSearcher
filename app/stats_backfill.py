"""One-time backfill: import stats from existing local log JSON files into stats SQLite DB."""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from app.config import LOGS_DIR, STATS_DB_PATH
from app.poisoned_logs import is_log_excluded
from app.stats_db import connect_stats_db, init_stats_db, rebuild_player_stats_agg, replace_stats_for_log

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


def _filter_log_paths_for_backfill(
    files: list[Path],
    *,
    min_log_id: int | None,
    max_log_id: int | None,
    skip_files: int,
) -> list[Path]:
    """
    Sub-sequence of sorted ``<id>.json`` paths.

    Filters apply in order: ``min_log_id``, ``max_log_id`` (on numeric stem), then drop the
    first ``skip_files`` entries (used to resume using the first number from a Progress line).
    """
    out = files
    if min_log_id is not None:
        out = [p for p in out if int(p.stem) >= min_log_id]
    if max_log_id is not None:
        out = [p for p in out if int(p.stem) <= max_log_id]
    sk = max(0, int(skip_files))
    if sk:
        if sk >= len(out):
            return []
        out = out[sk:]
    return out


def run_backfill(
    logs_dir: Path,
    db_path: Path,
    batch_size: int,
    *,
    min_log_id: int | None = None,
    max_log_id: int | None = None,
    skip_files: int = 0,
) -> None:
    files_all = _iter_log_files(logs_dir)
    files = _filter_log_paths_for_backfill(
        files_all,
        min_log_id=min_log_id,
        max_log_id=max_log_id,
        skip_files=skip_files,
    )
    logger.info(
        "Found %s local log JSON file(s) under %s; %s to process "
        "(min_log_id=%s max_log_id=%s skip_files=%s)",
        len(files_all),
        logs_dir,
        len(files),
        min_log_id,
        max_log_id,
        skip_files,
    )
    if not files:
        logger.warning("No log files match filters; nothing to do (stats DB unchanged).")
        return

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
                if is_log_excluded(log_id, logtext):
                    continue
                player_rows_total += replace_stats_for_log(conn, log_id, logtext)
            except (OSError, ValueError, TypeError) as e:
                parse_errors += 1
                logger.warning("Skipping %s due to parse/read error: %s", p.name, e)
                continue

            if processed % batch_size == 0:
                conn.commit()
                elapsed = max(0.001, time.time() - start)
                logger.info(
                    "Progress: %s/%s logs in this run, %s player rows inserted (%.1f logs/s)",
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
    try:
        n_agg = rebuild_player_stats_agg(conn)
        logger.info("player_stats_agg rebuilt: %s row(s)", n_agg)
    except Exception:
        logger.exception("player_stats_agg rebuild failed after backfill (run python -m app.rebuild_agg)")
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
    parser.add_argument(
        "--min-log-id",
        type=int,
        default=None,
        metavar="ID",
        help="Only import JSON logs whose numeric id is >= ID (after sorting)",
    )
    parser.add_argument(
        "--max-log-id",
        type=int,
        default=None,
        metavar="ID",
        help="Only import JSON logs whose numeric id is <= ID (after sorting)",
    )
    parser.add_argument(
        "--skip-files",
        type=int,
        default=0,
        metavar="N",
        help=(
            "Skip the first N files after sorting and min/max filters — use this to resume: "
            "set N to the first number from the last successful Progress line "
            "(logs in this run / …) so already-imported logs are not re-read"
        ),
    )
    args = parser.parse_args()

    logs_dir = Path(args.logs_dir)
    db_path = Path(args.db_path)
    batch_size = max(1, int(args.batch_size))
    if args.skip_files < 0:
        raise SystemExit("--skip-files must be >= 0")
    if args.min_log_id is not None and args.max_log_id is not None:
        if args.min_log_id > args.max_log_id:
            raise SystemExit("--min-log-id must be <= --max-log-id")
    if not logs_dir.exists() or not logs_dir.is_dir():
        raise SystemExit(f"Invalid --logs-dir: {logs_dir}")

    run_backfill(
        logs_dir,
        db_path,
        batch_size,
        min_log_id=args.min_log_id,
        max_log_id=args.max_log_id,
        skip_files=args.skip_files,
    )


if __name__ == "__main__":
    main()
