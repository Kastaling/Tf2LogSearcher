"""One-time backfill: import chat from existing local log JSON files into chat SQLite DB."""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from app.chat_db import connect_chat_db, init_chat_db, rebuild_alias_fts_if_needed, replace_chat_for_log
from app.config import CHAT_DB_PATH, LOGS_DIR

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
    conn = connect_chat_db(db_path)
    init_chat_db(conn)
    logger.info("Writing chat rows into %s", db_path)

    start = time.time()
    processed = 0
    inserted_messages = 0
    parse_errors = 0

    conn.execute("BEGIN")
    try:
        for p in files:
            processed += 1
            try:
                log_id = int(p.stem)
                logtext = json.loads(p.read_text(encoding="utf-8", errors="replace"))
                inserted_messages += replace_chat_for_log(conn, log_id, logtext)
            except (OSError, ValueError, TypeError) as e:
                parse_errors += 1
                logger.warning("Skipping %s due to parse/read error: %s", p.name, e)
                continue

            if processed % batch_size == 0:
                conn.commit()
                elapsed = max(0.001, time.time() - start)
                logger.info(
                    "Progress: %s/%s logs processed, %s chat rows inserted (%.1f logs/s)",
                    processed,
                    len(files),
                    inserted_messages,
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
        "Backfill complete: processed=%s inserted_rows=%s parse_errors=%s elapsed=%.1fs (%.1f logs/s)",
        processed,
        inserted_messages,
        parse_errors,
        elapsed,
        processed / elapsed,
    )
    logger.info("Rebuilding alias FTS index (may take a long time on large imports)...")
    rebuild_alias_fts_if_needed(db_path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill chat SQLite DB from existing local log JSON files."
    )
    parser.add_argument("--logs-dir", default=str(LOGS_DIR), help="Directory containing local <id>.json log files")
    parser.add_argument("--db-path", default=str(CHAT_DB_PATH), help="SQLite DB file path for chat data")
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
