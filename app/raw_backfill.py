"""
Backfill raw_events.db from existing raw log zip files on disk.
Processes all log_{id}.log.zip files in RAW_LOGS_DIR.

Usage: python -m app.raw_backfill [--raw-logs-dir ...] [--db-path ...] [--batch-size 200]
       [--min-log-id ID] [--max-log-id ID] [--skip-files N]
       [--failed-ids-file PATH] [--retry-failed PATH]
"""
from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import sys
import time
from pathlib import Path

from app.config import RAW_EVENTS_DB_PATH, RAW_LOGS_DIR
from app.raw_zip_io import extract_log_content_from_zip
from app.raw_db import (
    checkpoint_raw_db,
    connect_raw_db,
    init_raw_db,
    is_sqlite_corrupt_error,
    quick_raw_db_ok,
    replace_raw_events_for_log,
)
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

# Reconnect+retry attempts per log when corruption persists (quick_raw_db_ok can pass read-only).
_MAX_SQLITE_CORRUPT_RETRIES = 3


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


def _read_retry_log_ids(path: Path) -> list[int]:
    out: list[int] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        s = line.split("#", 1)[0].strip()
        if not s:
            continue
        try:
            out.append(int(s))
        except ValueError:
            continue
    return sorted(set(out))


def _append_failed_log_id(path: Path, log_id: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(f"{log_id}\n")


def _queue_rolled_back_logs(
    failed_ids_file: Path | None,
    batch_log_ids: list[int],
) -> int:
    """Append uncommitted batch log ids for --retry-failed; return count queued."""
    if not batch_log_ids:
        return 0
    if failed_ids_file is not None:
        for lid in batch_log_ids:
            _append_failed_log_id(failed_ids_file, lid)
    return len(batch_log_ids)


def _empty_agg() -> dict[str, int]:
    return {k: 0 for k in _AGG_KEYS}


def _rollback_savepoint(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK TO SAVEPOINT raw_backfill_log")
    except sqlite3.Error:
        pass


def _release_savepoint(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("RELEASE SAVEPOINT raw_backfill_log")
    except sqlite3.Error:
        pass


def _begin_tx(conn: sqlite3.Connection) -> None:
    conn.execute("BEGIN IMMEDIATE")


def _safe_rollback(conn: sqlite3.Connection | None) -> None:
    if conn is None:
        return
    try:
        conn.rollback()
    except sqlite3.Error:
        pass


def _reconnect_raw_db(db_path: Path) -> sqlite3.Connection:
    checkpoint_raw_db(db_path, truncate=True)
    if not quick_raw_db_ok(db_path):
        raise RuntimeError(
            f"raw_events.db at {db_path} is unreadable after WAL checkpoint. "
            "Stop web/downloader, run: sqlite3 downloader_state/raw_events.db "
            "\".recover\" | sqlite3 downloader_state/raw_events_recovered.db"
        )
    conn = connect_raw_db(db_path)
    init_raw_db(conn)
    return conn


def _recover_from_sqlite_corruption(
    conn: sqlite3.Connection | None,
    db_path: Path,
    log_id: int,
    *,
    in_batch: int,
    batch_log_ids: list[int],
    failed_ids_file: Path | None,
) -> tuple[sqlite3.Connection, int, int]:
    """Roll back uncommitted work, queue ids for retry, reconnect."""
    uncommitted = in_batch
    _safe_rollback(conn)
    try:
        if conn is not None:
            conn.close()
    except sqlite3.Error:
        pass
    rolled_back = _queue_rolled_back_logs(failed_ids_file, batch_log_ids)
    logger.error(
        "SQLite corruption detected at log %s (later failures are often "
        "cascade on the same connection). Rolling back %s uncommitted "
        "log(s) and reconnecting.",
        log_id,
        uncommitted,
    )
    if rolled_back and failed_ids_file is not None:
        logger.warning(
            "Queued %s rolled-back log id(s) for retry in %s",
            rolled_back,
            failed_ids_file,
        )
    conn = _reconnect_raw_db(db_path)
    _begin_tx(conn)
    return conn, uncommitted, rolled_back


def _process_one_log(
    conn: sqlite3.Connection,
    raw_dir: Path,
    log_id: int,
    agg: dict[str, int],
) -> None:
    zip_path = raw_dir / f"log_{log_id}.log.zip"
    zip_bytes = zip_path.read_bytes()
    content = extract_log_content_from_zip(zip_bytes)
    if content is None:
        raise OSError("could not read zip")
    parsed = parse_raw_log(log_id, content)
    conn.execute("SAVEPOINT raw_backfill_log")
    try:
        counts = replace_raw_events_for_log(conn, log_id, parsed)
    except Exception:
        _rollback_savepoint(conn)
        raise
    _release_savepoint(conn)
    for k in _AGG_KEYS:
        if k in counts:
            agg[k] += counts[k]


def run_backfill(
    raw_dir: Path,
    db_path: Path,
    batch_size: int,
    *,
    min_log_id: int | None = None,
    max_log_id: int | None = None,
    skip_files: int = 0,
    retry_failed: Path | None = None,
    failed_ids_file: Path | None = None,
) -> None:
    if retry_failed is not None:
        ids = _read_retry_log_ids(retry_failed)
        ids_all = ids
        logger.info(
            "Retry mode: %s log id(s) from %s",
            len(ids),
            retry_failed,
        )
    else:
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

    conn: sqlite3.Connection | None = None
    try:
        conn = connect_raw_db(db_path)
        init_raw_db(conn)
        total = len(ids)
        agg = _empty_agg()
        t0 = time.perf_counter()
        in_batch = 0
        batch_log_ids: list[int] = []
        processed = 0
        failed = 0

        _begin_tx(conn)
        for i, log_id in enumerate(ids, start=1):
            zip_path = raw_dir / f"log_{log_id}.log.zip"
            if not zip_path.is_file():
                logger.warning("Skip log %s: missing %s", log_id, zip_path.name)
                failed += 1
                if failed_ids_file is not None:
                    _append_failed_log_id(failed_ids_file, log_id)
                continue

            try:
                _process_one_log(conn, raw_dir, log_id, agg)
                processed += 1
            except OSError as e:
                logger.warning("Skip log %s: %s", log_id, e)
                failed += 1
                if failed_ids_file is not None:
                    _append_failed_log_id(failed_ids_file, log_id)
                continue
            except Exception as e:
                corrupt_retries = 0
                while is_sqlite_corrupt_error(e):
                    if corrupt_retries >= _MAX_SQLITE_CORRUPT_RETRIES:
                        logger.error(
                            "SQLite corruption persists for log %s after %s reconnect "
                            "attempt(s); queuing for --retry-failed and continuing.",
                            log_id,
                            corrupt_retries,
                        )
                        break
                    corrupt_retries += 1
                    try:
                        conn, uncommitted, rolled_back = _recover_from_sqlite_corruption(
                            conn,
                            db_path,
                            log_id,
                            in_batch=in_batch,
                            batch_log_ids=batch_log_ids,
                            failed_ids_file=failed_ids_file,
                        )
                    except RuntimeError as re_err:
                        logger.error("%s", re_err)
                        raise SystemExit(1) from re_err
                    processed -= uncommitted
                    failed += rolled_back
                    agg = _empty_agg()
                    in_batch = 0
                    batch_log_ids = []
                    try:
                        _process_one_log(conn, raw_dir, log_id, agg)
                        processed += 1
                        in_batch += 1
                        batch_log_ids.append(log_id)
                        e = None
                        break
                    except Exception as retry_err:
                        e = retry_err
                if e is None:
                    continue
                logger.warning("Failed log %s: %s", log_id, e)
                failed += 1
                if failed_ids_file is not None:
                    _append_failed_log_id(failed_ids_file, log_id)
                continue

            in_batch += 1
            batch_log_ids.append(log_id)
            if in_batch >= batch_size:
                conn.commit()
                checkpoint_raw_db(db_path, truncate=True)
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
                batch_log_ids = []
                _begin_tx(conn)

        if conn is not None:
            conn.commit()
            checkpoint_raw_db(db_path, truncate=True)
        elapsed = time.perf_counter() - t0
        logger.info(
            "Done: processed=%s failed=%s zip(s) in %.1fs (%.1f logs/s)",
            processed,
            failed,
            elapsed,
            processed / max(0.001, elapsed),
        )
        if failed and failed_ids_file is not None:
            logger.info(
                "Failed log ids appended to %s — retry with: "
                "python -m app.raw_backfill --retry-failed %s",
                failed_ids_file,
                failed_ids_file,
            )
    finally:
        if conn is not None:
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
    ap.add_argument(
        "--failed-ids-file",
        type=Path,
        default=None,
        metavar="PATH",
        help="Append failed log ids to PATH for later --retry-failed",
    )
    ap.add_argument(
        "--retry-failed",
        type=Path,
        default=None,
        metavar="PATH",
        help="Only re-parse log ids listed in PATH (from --failed-ids-file)",
    )
    args = ap.parse_args()

    batch_size = max(1, int(args.batch_size))
    if args.skip_files < 0:
        raise SystemExit("--skip-files must be >= 0")
    if args.min_log_id is not None and args.max_log_id is not None:
        if args.min_log_id > args.max_log_id:
            raise SystemExit("--min-log-id must be <= --max-log-id")
    if args.retry_failed is not None and not args.retry_failed.is_file():
        raise SystemExit(f"--retry-failed file not found: {args.retry_failed}")

    run_backfill(
        args.raw_logs_dir,
        args.db_path,
        batch_size,
        min_log_id=args.min_log_id,
        max_log_id=args.max_log_id,
        skip_files=args.skip_files,
        retry_failed=args.retry_failed,
        failed_ids_file=args.failed_ids_file,
    )


if __name__ == "__main__":
    main()
